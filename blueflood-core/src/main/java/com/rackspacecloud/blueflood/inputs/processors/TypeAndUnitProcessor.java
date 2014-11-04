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

import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.AsyncFunction;



public class TypeAndUnitProcessor extends FunctionWithThreadPool<MetricsCollection, MetricsCollection> {
        
    final IncomingMetricMetadataAnalyzer metricMetadataAnalyzer;
    
    public TypeAndUnitProcessor(ThreadPoolExecutor threadPool, IncomingMetricMetadataAnalyzer metricMetadataAnalyzer) {
        super(threadPool);
        this.metricMetadataAnalyzer = metricMetadataAnalyzer;
    }
    
    public void apply(final MetricsCollection input) throws Exception {
        getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                Collection<IncomingMetricException> problems = metricMetadataAnalyzer.scanMetrics(input.toMetrics());
                for (IncomingMetricException problem : problems)
                    // TODO: this is where a system annotation should be raised.
                    getLogger().warn(problem.getMessage());
                return input;
            }
        });
    }

static public void main2(String args[]) throws Exception
{
    TypeAndUnitProcessor typeAndUnitProcessor;
    IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

    typeAndUnitProcessor = 
        new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10), metricMetadataAnalyzer);
}
static public void main3(String args[]) throws Exception
{
    TypeAndUnitProcessor typeAndUnitProcessor;
    IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    Logger log = LoggerFactory.getLogger(TypeAndUnitProcessor.class);

    typeAndUnitProcessor = 
            new TypeAndUnitProcessor(new ThreadPoolBuilder()
                .withName("Metric type and unit processing")
                .withCorePoolSize(10)
                .withMaxPoolSize(10)
                .build(),
                metricMetadataAnalyzer
            );
    typeAndUnitProcessor.withLogger(log);

}
static public void main(String args[]) throws Exception
{
    TypeAndUnitProcessor typeAndUnitProcessor;
    IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    Logger log = LoggerFactory.getLogger(TypeAndUnitProcessor.class);
/*
    AsyncFunction<MetricsCollection, MetricsCollection> t =
        new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10), metricMetadataAnalyzer)
            .withLogger(log);


    FunctionWithThreadPool<MetricsCollection, MetricsCollection> t =
        new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10), metricMetadataAnalyzer)
            .withLogger(log);

    typeAndUnitProcessor = 
            new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10),
                metricMetadataAnalyzer
            );
    typeAndUnitProcessor.withLogger(log);

    typeAndUnitProcessor = 
            new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10),
                metricMetadataAnalyzer
            ).<MetricsCollection, MetricsCollection>withLogger(log);

    FunctionWithThreadPool<MetricsCollection, MetricsCollection> t2 =
            new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10),
                metricMetadataAnalyzer
            ).<MetricsCollection, MetricsCollection>withLogger(log);

*/

    FunctionWithThreadPool<MetricsCollection, MetricsCollection> t2 =
            new TypeAndUnitProcessor(new ScheduledThreadPoolExecutor(10),
                metricMetadataAnalyzer
            ).withLogger(log);

}

}

