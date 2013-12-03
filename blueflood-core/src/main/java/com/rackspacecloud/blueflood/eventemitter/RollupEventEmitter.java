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

import com.github.nkzawa.emitter.Emitter;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.Locator;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.*;

public class RollupEventEmitter {
    private static Emitter emitterInstance = new Emitter();
    private static ThreadPoolExecutor eventExecutors;
    private static int numberOfWorkerThreads = 5;

    static {
        eventExecutors = new ThreadPoolBuilder()
                         .withName("EventEmitter ThreadPool")
                         .withCorePoolSize(numberOfWorkerThreads)
                         .withMaxPoolSize(numberOfWorkerThreads)
                         .withUnboundedQueue()
                         .build();
    }

    public static Emitter getEmitterInstance() {
        return emitterInstance;
    }

    public static void emit(Object... eventPayload) {
        eventExecutors.execute(new RollupEmissionWork(eventPayload));
    }

    public static ThreadPoolExecutor getEventExecutors() {
        return eventExecutors;
    }
}
