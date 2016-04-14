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

package com.rackspacecloud.blueflood.io.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BluefloodEnumRollup;
import com.rackspacecloud.blueflood.types.IMetric;

import java.io.IOException;
import java.util.Collection;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Astyanax driver
 */
public class AstyanaxPreaggregatedMetricsRW extends AbstractMetricsRW {

    /**
     * Inserts a collection of metrics to the metrics_preaggregated_full column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Inserts a collection of rolled up metrics to the metrics_preaggregated_{granularity} column family
     *
     * @param metrics
     * @throws IOException
     */
    public void insertRollups(Collection<IMetric> metrics, Granularity granularity) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertMetrics(metrics, CassandraModel.getColumnFamily(BluefloodEnumRollup.class, granularity));
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }
}
