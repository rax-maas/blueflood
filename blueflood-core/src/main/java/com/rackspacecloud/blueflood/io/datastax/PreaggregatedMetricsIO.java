/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.*;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Datastax driver
 */
public class PreaggregatedMetricsIO extends AbstractMetricsIO {

    private static final Logger LOG = LoggerFactory.getLogger(PreaggregatedMetricsIO.class);

    private final DatastaxEnumIO enumIO = new DatastaxEnumIO();

    private final Map<RollupType, AsyncWriter> rollupTypeToWriterMap = new HashMap<RollupType, AsyncWriter>() {{
        put(RollupType.ENUM, enumIO);
        // more IO mapping here
        // put(RollupType.COUNTER, counterIO);
    }};

    /**
     * Inserts a collection of metrics to the metrics_preaggregated_full column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics) throws IOException {
        insertRollups(metrics, Granularity.FULL);

        // TODO: insert locator
    }

    /**
     * Inserts a collection of rolled up metrics to the metrics_preaggregated_{granularity} column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertRollups(Collection<IMetric> metrics, Granularity granularity) throws IOException {
        List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
        Multimap<Locator, IMetric> map = asMultimap(metrics);
        for (Locator locator : map.keySet()) {
            for (IMetric metric : map.get(locator)) {
                RollupType rollupType = metric.getRollupType();
                if ( rollupType == null ) {
                    rollupType = RollupType.BF_BASIC;
                }

                // lookup the right writer
                AsyncWriter writer = rollupTypeToWriterMap.get(rollupType);
                if ( writer == null ) {
                    throw new InvalidDataException(
                            String.format("insertMetrics(locator=%s): unsupported preaggregated rollupType=%s",
                                    locator, rollupType.name()));
                }

                if ( !(metric.getMetricValue() instanceof Rollup) ) {
                    throw new InvalidDataException(
                            String.format("insertMetrics(locator=%s): metric value %s is not type Rollup",
                                    locator, metric.getMetricValue().getClass().getSimpleName())
                    );
                }
                futures.add(writer.putAsync(locator, metric.getCollectionTime(), (Rollup)metric.getMetricValue(), granularity));
            }
        }

        for (ResultSetFuture future : futures) {
            try {
                future.get().all();
            } catch (InterruptedException ex) {
                Instrumentation.markWriteError();
                LOG.error("Interrupted error writing preaggregated metric", ex);
            } catch (ExecutionException ex) {
                Instrumentation.markWriteError();
                LOG.error("Execution error writing preaggregated metric", ex);
            }
        }
    }

}
