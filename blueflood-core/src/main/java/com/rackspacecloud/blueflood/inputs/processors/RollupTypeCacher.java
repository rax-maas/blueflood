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

package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class RollupTypeCacher extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {

    private static final Logger log = LoggerFactory.getLogger(RollupTypeCacher.class);
    private static final String cacheKey = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();
    private final Timer recordDurationTimer = Metrics.timer(RollupTypeCacher.class, "Record Duration");

    private final MetadataCache cache;
    private final boolean isAsync;
    
    public RollupTypeCacher(ThreadPoolExecutor executor, MetadataCache cache, boolean async) {
        super(executor);
        this.cache = cache;
        this.isAsync = async;
    }

    @Override
    public ListenableFuture<MetricsCollection> apply(MetricsCollection input) throws Exception {
        return isAsync ? asynchronous(input) : synchronous(input);
    }
    
    private ListenableFuture<MetricsCollection> asynchronous(final MetricsCollection input) {

        getThreadPool().submit(new Runnable() {
            @Override
            public void run() {
                recordWithTimer(input);
            }
        });
        return new NoOpFuture<MetricsCollection>(input);
    }
    
    private ListenableFuture<MetricsCollection> synchronous(final MetricsCollection input) {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            @Override
            public MetricsCollection call() throws Exception {
                recordWithTimer(input);
                return input;
            }
        });
    }
    
    private void recordWithTimer(MetricsCollection input) {
        final Timer.Context recordDurationContext = recordDurationTimer.time();

        try {
            record(input);
        } finally {
            recordDurationContext.stop();
        }
    }

    private void record(MetricsCollection input) {
        for (IMetric metric : input.toMetrics()) {
            try {
                cache.put(metric.getLocator(), cacheKey, metric.getRollupType().toString());
            } catch (Exception ex) {
                log.warn("Exception updating cache with rollup type ", ex);
            }
        }
    }
}
