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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.Metric;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import javax.security.auth.login.Configuration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<List<Metric>>> {

    private final List<DiscoveryIO> discoveryIOs = new ArrayList<DiscoveryIO>();
    private final Map<Class<? extends DiscoveryIO>, Timer> writeDurationTimers= new HashMap<Class<? extends DiscoveryIO>, Timer>();

    public DiscoveryWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }

    public void registerIO(DiscoveryIO io) {
        discoveryIOs.add(io);
        writeDurationTimers.put(io.getClass(),
                Metrics.newTimer(io.getClass(), "DiscoveryWriter Write Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
                );
    }

    public ListenableFuture<List<Boolean>> processMetrics(List<List<Metric>> input) {
        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();
        for (final List<Metric> metrics : input) {
            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    boolean success = true;
                        for (DiscoveryIO io : discoveryIOs) {
                            TimerContext actualWriteCtx = writeDurationTimers.get(io.getClass()).time();
                            try {
                                io.insertDiscovery(metrics);
                            } catch (Exception ex) {
                                getLogger().error(ex.getMessage(), ex);
                                success = false;
                            }
                            // should we time failed attempts?
                            actualWriteCtx.stop();
                        }
                        return success;
                }
            });
            resultFutures.add(futureBatchResult);
        }
        return Futures.allAsList(resultFutures);
    }

    public ListenableFuture<List<List<Metric>>> apply(List<List<Metric>> input) {
        processMetrics(input);
        // we don't need all metrics to finish being inserted into the discovery backend
        // before moving onto the next step in the processing chain.
        return new NoOpFuture<List<List<Metric>>>(input);
    }
}
