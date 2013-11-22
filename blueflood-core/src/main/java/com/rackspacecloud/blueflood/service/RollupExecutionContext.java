package com.rackspacecloud.blueflood.service;

import java.util.concurrent.atomic.AtomicLong;

// Context of execution for a single shard, representing many rollups of a given granularity.
public class RollupExecutionContext {
    private final AtomicLong readCounter;
    private final Thread owner;
    private AtomicLong writeCounter;

    public RollupExecutionContext(Thread owner) {
        this.owner = owner;
        this.readCounter = new AtomicLong(0L);
        this.writeCounter = new AtomicLong(0L);
    }

    void decrementReadCounter() {
        readCounter.decrementAndGet();
        owner.interrupt(); // this is what interrupts that long sleep in LocatorFetchRunnable.
    }

    void decrementWriteCounter(long count) {
        writeCounter.addAndGet((-1) * count);
        owner.interrupt();
    }

    void incrementReadCounter() {
        readCounter.incrementAndGet();
    }

    void incrementWriteCounter() {
        writeCounter.incrementAndGet();
    }

    boolean doneReading() {
        return readCounter.get() == 0;
    }

    boolean doneWriting() {
        return writeCounter.get() == 0;
    }
}
