package com.rackspacecloud.blueflood.service;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Context of execution for all the rollup running under a single slot check.
 */
public class RollupExecutionContext {
    private final Thread slotCheckThread;
    private final AtomicLong readCounter;
    private final AtomicLong writeCounter;
    private final AtomicBoolean successful = new AtomicBoolean(true);

    public RollupExecutionContext(Thread slotCheckThread) {
        this.slotCheckThread = slotCheckThread;
        this.readCounter = new AtomicLong(0L);
        this.writeCounter = new AtomicLong(0L);
    }

    void decrementReadCounter() {
        readCounter.decrementAndGet();
        slotCheckThread.interrupt(); // this is what interrupts that long sleep in LocatorFetchRunnable.
    }

    void decrementWriteCounter(long count) {
        writeCounter.addAndGet((-1) * count);
        slotCheckThread.interrupt();
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

    boolean wasSuccessful() {
        return successful.get();
    }

    void markUnsuccessful(Throwable t) {
        successful.set(false);
    }
}
