package com.rackspacecloud.blueflood.eventemitter;

import com.github.nkzawa.emitter.Emitter;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;

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

    // TODO : Generalize this later to emit any event
    public static void emit(Object... eventPayload) {
        eventExecutors.execute(new RollupEmissionWork(eventPayload));
    }
}
