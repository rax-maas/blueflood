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

import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TypeAndUnitProcessor extends AsyncFunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    final IncomingMetricMetadataAnalyzer metricMetadataAnalyzer;
    
    public TypeAndUnitProcessor(ThreadPoolExecutor threadPool, IncomingMetricMetadataAnalyzer metricMetadataAnalyzer) {
        super(threadPool);
        this.metricMetadataAnalyzer = metricMetadataAnalyzer;
    }
    
    public ListenableFuture<MetricsCollection> apply(final MetricsCollection input) throws Exception {
        getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                Collection<IncomingMetricException> problems = metricMetadataAnalyzer.scanMetrics(input.getMetrics());
                for (IncomingMetricException problem : problems)
                    // TODO: this is where a system annotation should be raised.
                    getLogger().warn(problem.getMessage());
                return input;
            }
        });
        
        // this one is asynchronous. so we let it do its job offline in the threadpool, but return a future that is
        // immediately done.
        return new NoOpFuture<MetricsCollection>(input);
    }
}