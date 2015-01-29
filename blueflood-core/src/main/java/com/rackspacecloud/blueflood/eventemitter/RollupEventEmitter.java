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

package com.rackspacecloud.blueflood.eventemitter;

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.ModuleLoader;

import java.util.List;
import java.util.concurrent.*;

public class RollupEventEmitter extends Emitter<RollupEvent> {
    private static final int numberOfWorkers = 5;
    public static final String ROLLUP_EVENT_NAME = "rollup".intern();
    private static ThreadPoolExecutor eventExecutors;
    private static final RollupEventEmitter instance = new RollupEventEmitter();
    private static final DiscoveryIO discoveryHandler = ModuleLoader.getInstance().loadElasticIOModule();

    private RollupEventEmitter() {
        eventExecutors = new ThreadPoolBuilder()
                .withName("RollupEventEmitter ThreadPool")
                .withCorePoolSize(numberOfWorkers)
                .withMaxPoolSize(numberOfWorkers)
                .withUnboundedQueue()
                .build();
    }

    public static RollupEventEmitter getInstance() { return instance; }

    @Override
    public Future emit(final String event, final RollupEvent... eventPayload) {
        if (this.hasListeners(event)) {
            for (RollupEvent rollupEvent : eventPayload) {
                rollupEvent.setUnit(getUnits(rollupEvent.getLocator()));
            }
        }

        //TODO: This hack will go away after Kafka Serializer is made generic
        if(eventPayload[0].getRollup() instanceof BasicRollup) {
            return eventExecutors.submit(new Runnable() {
                @Override
                public void run() {
                    RollupEventEmitter.super.emit(event, eventPayload);
                }
            });
        }
        return null;
    }

    private String getUnits(Locator locator) {
        List<SearchResult> results;
        try {
            results = discoveryHandler.search(locator.getTenantId(), locator.getMetricName());
        } catch (Exception e) {
            return "UNKNOWN";
        }
        return results.get(0).getUnit();
    }
}
