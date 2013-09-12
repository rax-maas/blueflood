package com.rackspacecloud.blueflood.service;

import java.util.concurrent.atomic.AtomicLong;

public class RollupExecutionContext {
    private final AtomicLong counter;
    private final Thread owner;

    public RollupExecutionContext(Thread owner) {
        this.owner = owner;
        this.counter = new AtomicLong(0L);
    }

    void decrement() {
        counter.decrementAndGet();
        owner.interrupt(); // this is what interrupts that long sleep in LocatorFetchRunnable.
    }

    void increment() {
        counter.incrementAndGet();
    }

    boolean done() {
        return counter.get() == 0;
    }
}
