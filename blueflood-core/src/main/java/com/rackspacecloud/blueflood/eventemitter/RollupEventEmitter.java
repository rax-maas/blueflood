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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.*;

/**
 * @todo {@code RollupEventEmitter} should be modified so that it can be used
 * as a drop-in replacement for {@link Emitter<RollupEvent>}.
 *
 * The {@code RollupEventEmitter} class violates the Liskov substitution
 * principle, in that it doesn't behave the same as
 * {@link Emitter<RollupEvent>}. Moreover, the differences in behavior depend
 * upon particular details of the inputs.
 *
 * For example, if there is no listener registered for
 * {@link RollupEventEmitter#ROLLUP_EVENT_NAME}, then *ALL* events are silently
 * ignored. No other listeners will ever be notified of their events, even
 * events of other names.
 *
 * Also, if any event doesn't have a {@link RollupEvent#rollup} of type
 * {@link BasicRollup}, that event will be silently ignored. This is the case
 * if the event's {@link RollupEvent#rollup} field is {@code null}, or if it's
 * another rollup type
 *
 * @todo Merge {@code RollupEventEmitter} and {@link Emitter<>} into a single
 * class. {@link Emitter<>} doesn't appear to be used anywhere else.
 *
 * @todo Change the {@code eventPayload}/{@code args} parameter of
 * {@link RollupEventEmitter#emit} from an array to a single item. There don't
 * appear to be any parts of the codebase that use more than a single event
 * argument.
 */
public class RollupEventEmitter extends Emitter<RollupEvent> {
    private static final Logger log = LoggerFactory.getLogger(ModuleLoader.class);
    private static final int numberOfWorkers = 5;
    public static final String ROLLUP_EVENT_NAME = "rollup".intern();
    private final ExecutorService eventExecutors;
    private static final RollupEventEmitter instance = new RollupEventEmitter();

    public RollupEventEmitter() {
        this(new ThreadPoolBuilder()
                .withName("RollupEventEmitter ThreadPool")
                .withCorePoolSize(numberOfWorkers)
                .withMaxPoolSize(numberOfWorkers)
                .withUnboundedQueue()
                .build());
    }
    @VisibleForTesting
    public RollupEventEmitter(ExecutorService executor) {
        eventExecutors = executor;
    }

    public static RollupEventEmitter getInstance() { return instance; }

    @Override
    public Future emit(final String event, final RollupEvent... eventPayload) {
        //TODO: This hack will go away after Kafka Serializer is made generic
        Future emitFuture = null;
        if(eventPayload[0].getRollup() instanceof BasicRollup && super.hasListeners(ROLLUP_EVENT_NAME)) {
            emitFuture = eventExecutors.submit(new Callable() {
                @Override
                public Future call() {
                    if (Util.shouldUseESForUnits()) {
                        final DiscoveryIO discoveryIO = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);
                        // TODO: Sync for now, but we will have to make it async eventually
                        Lists.transform(Arrays.asList(eventPayload), new Function<RollupEvent, RollupEvent>() {
                            @Override
                            public RollupEvent apply(RollupEvent event) {
                                String unit;
                                try {
                                    unit = discoveryIO.search(event.getLocator().getTenantId(), event.getLocator().getMetricName()).get(0).getUnit();
                                } catch (Exception e) {
                                    log.warn("Exception encountered while getting units out of ES : %s", e.getMessage());
                                    unit = Util.UNKNOWN;
                                }
                                event.setUnit(unit);
                                return event;
                            }
                        });
                    }
                    return RollupEventEmitter.super.emit(event, eventPayload);
                }
            });
        }
        return emitFuture;
    }
}
