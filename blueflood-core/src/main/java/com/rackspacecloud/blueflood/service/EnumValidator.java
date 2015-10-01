/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.types.BluefloodEnumRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/** This class handles collecting enum values for specific metrics, via their locators, and checking whether
    a uniqueness count of the enum values have reached a certain threshold.  If it has, the class mark the metric
    as bad by inserting its locator into the metrics_excess_enums table.  If it hasn't reached the threshold, then the class
    will create/update the elastic search "enums" index for the metric, which will contain a list of all unique
    enum values that has been ingested for it.
**/
public class EnumValidator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EnumValidator.class);
    private static final Configuration config = Configuration.getInstance();
    private static final DiscoveryIO discoveryIO = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
    private static final int ENUM_UNIQUE_VALUES_THRESHOLD = config.getIntegerProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD);
    private static final int ENUM_VALIDATOR_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.ENUM_VALIDATOR_THREADS);

    HashSet<Locator> locators;

    public EnumValidator(Set<Locator> locators) {
        this.locators = (HashSet<Locator>) locators;
    }

    @Override
    public void run() {

        ExecutorService taskExecutor = new ThreadPoolBuilder().withUnboundedQueue()
                .withCorePoolSize(ENUM_VALIDATOR_THREADS)
                .withMaxPoolSize(ENUM_VALIDATOR_THREADS)
                .withName("Enum Validator Task Executor")
                .build();

        for (final Locator locator : locators) {
            // start async read for enum values of locator from cassandra
            Future<ColumnList<Long>> enumHashValuesFuture = taskExecutor.submit(new Callable() {
                @Override
                public ColumnList<Long> call() throws Exception {
                    return AstyanaxReader.getInstance().getColumnsFromEnumCF(locator);
                }

            });

            // start async read for enum values of locator from elastic search
            Future<List<SearchResult>> esEnumValuesFuture = taskExecutor.submit(new Callable() {
                @Override
                public List<SearchResult> call() throws Exception {
                    return discoveryIO.search(locator.getTenantId(), locator.getMetricName());
                }
            });


            Map<Long, String> enumStringValues = null;
            List<SearchResult> esEnumValues = null;
            try {
                // values from cassandra db
                ColumnList<Long> enumHashValues = enumHashValuesFuture.get();
                if ((enumHashValues != null) && (enumHashValues.size() > 0)) {
                    enumStringValues = AstyanaxReader.getInstance().getEnumValueFromHash(enumHashValues);
                    log.debug(String.format("Enum values for metric %s: %s", locator.toString(), enumStringValues.toString()));
                }
                else {
                    log.debug(String.format("No enum values found for metric %s", locator.toString()));
                }

                // values from elasticsearch
                esEnumValues = esEnumValuesFuture.get();

                validateThresholdAndIndex(locator, enumStringValues, esEnumValues);

            } catch (InterruptedException e) {
                log.error("Interrupted Exception during query of Enum metrics in EnumValidator", e);
            } catch (ExecutionException e) {
                log.error("Execution Exception during query of Enum metrics in EnumValidator", e);
            }
            finally {
                taskExecutor.shutdown();
            }
        }
    }

    private void validateThresholdAndIndex(Locator locator, Map<Long, String> enumStringValues, List<SearchResult> esEnumValues) {

        if ((enumStringValues != null) && (enumStringValues.size() > ENUM_UNIQUE_VALUES_THRESHOLD)) {
            // if values_from_cassandra.count > threshold,
            // write locator to bad metric table
            try {
                AstyanaxWriter.getInstance().writeExcessEnumMetric(locator);
            } catch (ConnectionException e) {
                log.error("Failed to write bad metric {} to table; {}", locator.toString(), e.getMessage());
            }
        }
        else {
            // create or update index of metric with enum values if cassandra and elasticsearch values are different
            // get arraylist of cassandra enum values
            ArrayList<String> dbValues = null;
            if ((enumStringValues != null) && (enumStringValues.size() > 0)) {
                dbValues = new ArrayList<String>(enumStringValues.values());
            }

            // get arraylist of elasticsearch enum values
            ArrayList<String> esValues = null;
            if ((esEnumValues != null) && (esEnumValues.get(0) != null)) {
                esValues = esEnumValues.get(0).getEnumValues();
            }

            // sort the two array lists of enum values
            if (dbValues != null) Collections.sort(dbValues);
            if (esValues != null) Collections.sort(esValues);

            // verify equals
            if (((dbValues != null) && (!dbValues.equals(esValues))) ||
                ((dbValues == null) && (esValues != null) && (esValues.size() > 0)))
            {
                // not equal, create or update enums index in elastic search
                BluefloodEnumRollup rollupWithEnumValues = createRollupWithEnumValues(dbValues);
                IMetric enumMetric = new PreaggregatedMetric(0, locator, null, rollupWithEnumValues);
                try {
                    discoveryIO.insertDiscovery(enumMetric);
                }
                catch (Exception e) {
                    log.error("Failed to write update elasticsearch enums index for {}; {}", locator.toString(), e.getMessage());
                }
            }
        }
    }

    private BluefloodEnumRollup createRollupWithEnumValues(ArrayList<String> enumValues) {
        BluefloodEnumRollup rollup = new BluefloodEnumRollup();
        for (String val : enumValues) {
            rollup = rollup.withEnumValue(val);
        }
        return rollup;
    }
}
