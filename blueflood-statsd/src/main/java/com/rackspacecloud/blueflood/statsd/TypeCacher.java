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

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TypeCacher extends AsyncFunctionWithThreadPool<StatCollection, StatCollection> {
    private static final Logger log = LoggerFactory.getLogger(TypeCacher.class);
    
    private final MetadataCache cache; 
    
    public TypeCacher(ThreadPoolExecutor executor, MetadataCache cache) {
        super(executor);
        this.cache = cache;
    }

    @Override
    public ListenableFuture<StatCollection> apply(final StatCollection input) throws Exception {
        
        return getThreadPool().submit(new Callable<StatCollection>() {
            @Override
            public StatCollection call() throws Exception {
                
                for (RollupType type : RollupType.SIMPLE_TYPES) {
                    for (Stat stat : input.getStats(type)) {
                        cache.put(stat.getLocator(), MetricMetadata.ROLLUP_TYPE.name(), type.toString());
                    }
                }
                
                for (Locator locator : input.getTimerStats().keySet()) {
                    cache.put(locator, MetricMetadata.ROLLUP_TYPE.name(), RollupType.TIMER.toString());
                }
                
                for (Locator locator : input.getCounterStats().keySet()) {
                    cache.put(locator, MetricMetadata.ROLLUP_TYPE.name(), RollupType.COUNTER.toString());
                }
                
                return input;
            }
        });
    }
}
