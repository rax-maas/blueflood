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

package com.rackspacecloud.blueflood.inputs.processors;

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.TtlCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TtlAffixer extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    private final TtlCache cache;
    
    public TtlAffixer(ThreadPoolExecutor threadPool, TtlCache cache) {
        super(threadPool);
        this.cache = cache;
    }
    
    public ListenableFuture<MetricsCollection> apply(final MetricsCollection metrics) {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                for (Metric metric : metrics.getMetrics()) {
                    ColumnFamily CF = CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL);
                    metric.setTtlInSeconds((int)cache.getTtl(metric.getLocator().getTenantId(),
                            CF).toSeconds());
                }
                return metrics;
            }
        });
    }
}
