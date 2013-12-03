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
