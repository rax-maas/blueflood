/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import java.io.IOException;
import java.util.Collection;

public class AstyanaxMetricsWriter implements IMetricsWriter {
    @Override
    public void insertFullMetrics(Collection<Metric> metrics) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertFull(metrics);
        } catch (ConnectionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void insertPreaggreatedMetrics(Collection<IMetric> metrics) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        } catch (ConnectionException e) {
            throw new IOException(e);
        }
    }
}