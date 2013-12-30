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
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;


import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class MetricsWriter extends AsyncFunctionWithThreadPool<StatCollection, Multimap<RollupType, IMetric>> {
    
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    
    public MetricsWriter(ThreadPoolExecutor executor) {
        super(executor);
    }
    
    public MetricsWriter withWriter(AstyanaxWriter writer) {
        this.writer = writer;
        return this;
    }

    @Override
    public ListenableFuture<Multimap<RollupType, IMetric>> apply(final StatCollection input) throws Exception {
        return getThreadPool().submit(new Callable<Multimap<RollupType, IMetric>>() {
            @Override
            public Multimap<RollupType, IMetric> call() throws Exception {
                Multimap<RollupType, IMetric> metrics = Conversions.asMetrics(input);
                // there will be no string metrics, so we can get away with assuming CF_METRICS_FULL.
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(RollupType.BF_BASIC)), CassandraModel.getColumnFamily(SimpleNumber.class, Granularity.FULL));
                
                // the rest of these calls deal with preaggregated metrics.
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(RollupType.COUNTER)), CassandraModel.getColumnFamily(CounterRollup.class, Granularity.FULL));
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(RollupType.SET)), CassandraModel.getColumnFamily(SetRollup.class, Granularity.FULL));
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(RollupType.GAUGE)), CassandraModel.getColumnFamily(GaugeRollup.class, Granularity.FULL));
                writer.insertMetrics(new ArrayList<IMetric>(metrics.get(RollupType.TIMER)), CassandraModel.getColumnFamily(TimerRollup.class, Granularity.FULL));
                return metrics;
            }
        });
    }
}
