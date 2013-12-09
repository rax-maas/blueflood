/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.statsd;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.StatType;


import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class MetricsWriter extends AsyncFunctionWithThreadPool<StatCollection, Multimap<StatType, IMetric>> {
    
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    
    public MetricsWriter(ThreadPoolExecutor executor) {
        super(executor);
    }
    
    public MetricsWriter withWriter(AstyanaxWriter writer) {
        this.writer = writer;
        return this;
    }

    @Override
    public ListenableFuture<Multimap<StatType, IMetric>> apply(final StatCollection input) throws Exception {
        return getThreadPool().submit(new Callable<Multimap<StatType, IMetric>>() {
            @Override
            public Multimap<StatType, IMetric> call() throws Exception {
                Multimap<StatType, IMetric> metrics = Conversions.asMetrics(input);
                // there will be no string metrics, so we can get away with assuming CF_METRICS_FULL.
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(StatType.UNKNOWN)), AstyanaxIO.CF_METRICS_FULL);
                
                // the rest of these calls deal with preaggregated metrics.
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(StatType.COUNTER)), AstyanaxIO.CF_METRICS_PREAGGREGATED_FULL);
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(StatType.SET)), AstyanaxIO.CF_METRICS_PREAGGREGATED_FULL);
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(StatType.GAUGE)), AstyanaxIO.CF_METRICS_PREAGGREGATED_FULL);
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(StatType.TIMER)), AstyanaxIO.CF_METRICS_PREAGGREGATED_FULL);
                return metrics;
            }
        });
    }
}
