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
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.StatType;
import com.rackspacecloud.blueflood.utils.Util;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class ContextUpdater extends AsyncFunctionWithThreadPool<Multimap<StatType, IMetric>, Multimap<StatType, IMetric>> {
    private final ScheduleContext context;
    
    public ContextUpdater(ScheduleContext context, ThreadPoolExecutor executor) {
        super(executor);
        this.context = context;
    }
    
    @Override
    public ListenableFuture<Multimap<StatType, IMetric>> apply(final Multimap<StatType, IMetric> input) throws Exception {
        return getThreadPool().submit(new Callable<Multimap<StatType, IMetric>>() {
            @Override
            public Multimap<StatType, IMetric> call() throws Exception {
                for (StatType type : input.keySet())
                    for (IMetric metric : input.get(type))
                        context.update(metric.getCollectionTime(), Util.computeShard(metric.getLocator().toString()));
                return input;
            }
        });
    }
}
