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
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
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

/** This class handles collecting enum values for specific metrics, via their locators, and checking whether
    a uniqueness count of the enum values have reached a certain threshold.  If it has, the class mark the metric
    as bad by inserting its locator into the proper cassandra column family.  If it hasn't reached the threshold, then it
    will create or update the elasticsearch "enums" index for the metric.
**/
public class EnumValidator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(EnumValidator.class);
    private static final Configuration config = Configuration.getInstance();
    private static final int ENUM_UNIQUE_VALUES_THRESHOLD = config.getIntegerProperty(CoreConfig.ENUM_UNIQUE_VALUES_THRESHOLD);
    private Set<Locator> locators;

    private AstyanaxReader reader = null;
    private AstyanaxWriter writer = null;
    private DiscoveryIO discoveryIO = null;

    public EnumValidator(Set<Locator> locators) {
        this.locators = locators;
    }

    @Override
    public void run() {
        if (locators == null) return;

        Map<Locator, ArrayList<String>> locatorEnums = getReader().getEnumStringMappings(new ArrayList(locators));
        for (final Locator locator : locatorEnums.keySet()) {
            // validate enum values count and write to index or bad metric
            validateThresholdAndWrite(locator, locatorEnums.get(locator));
        }
    }

    private void validateThresholdAndWrite(Locator locator, ArrayList<String> currentEnumValues) {
        // check if count of current enum values for the metric exceed a configurable threshold number
        log.debug(String.format("EnumValidator validating locator %s", locator.toString()));

        // if exceeded, mark metric as bad, else index enum values in elasticsearch
        if ((currentEnumValues != null) && (currentEnumValues.size() > ENUM_UNIQUE_VALUES_THRESHOLD)) {
            // count of current enum values of metric exceeded threshold, bad metric
            // write locator to bad metric table
            try {
                getWriter().writeExcessEnumMetric(locator);
            } catch (ConnectionException e) {
                log.error(String.format("Exception writing bad metric %s: %s", locator.toString(), e.getMessage()), e);
            }
        }
        else {
            // not bad metric, create or update enums index of metric in elasticsearch if different from cassandra
            // search for metric from elastic search
            List<SearchResult> esSearchResult = null;
            try {
                esSearchResult = getDiscoveryIO().search(locator.getTenantId(), locator.getMetricName());
            }
            catch (Exception e) {
                log.error(String.format("Exception retrieving enum values from elasticsearch for %s: %s", locator.toString(), e.getMessage()), e);
            }

            // get elasticsearch enum values from top search results of exact match
            ArrayList<String> elasticsearchEnumValues = null;
            if ((esSearchResult != null) && (esSearchResult.size() > 0)) {
                elasticsearchEnumValues = esSearchResult.get(0).getEnumValues();
            }

            // sort the two array lists of enum values
            if (currentEnumValues != null) Collections.sort(currentEnumValues);
            if (elasticsearchEnumValues != null) Collections.sort(elasticsearchEnumValues);

            // compare two list of enum values and right if CF has enum values for the metric and it doesn't equal to the values in elasticsearch
            if ((currentEnumValues != null) && (currentEnumValues.size() > 0) && (!currentEnumValues.equals(elasticsearchEnumValues)))
            {
                // if not equal, create or update enums index in elastic search
                BluefloodEnumRollup rollupWithEnumValues = createRollupWithEnumValues(currentEnumValues);
                IMetric enumMetric = new PreaggregatedMetric(0, locator, null, rollupWithEnumValues);
                try {
                    getDiscoveryIO().insertDiscovery(enumMetric);
                }
                catch (Exception e) {
                    log.error(String.format("Exception writing enums index to elasticsearch for %s: %s", locator.toString(), e.getMessage()), e);
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

    public AstyanaxReader getReader() {
        if (this.reader == null) {
            this.reader = AstyanaxReader.getInstance();
        }
        return this.reader;
    }

    public void setReader(AstyanaxReader reader) {
        this.reader = reader;
    }

    public AstyanaxWriter getWriter() {
        if (this.writer == null) {
            this.writer = AstyanaxWriter.getInstance();
        }
        return this.writer;
    }

    public void setWriter(AstyanaxWriter writer) {
        this.writer = writer;
    }

    public DiscoveryIO getDiscoveryIO() {
        if (this.discoveryIO == null) {
            this.discoveryIO = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
        }
        return this.discoveryIO;
    }

    public void setDiscoveryIO(DiscoveryIO discoveryIO) {
        this.discoveryIO = discoveryIO;
    }
}
