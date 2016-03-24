package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class StubbedLocatorFetchRunnable extends LocatorFetchRunnable {

    public StubbedLocatorFetchRunnable(ScheduleContext scheduleCtx,
                         SlotKey destSlotKey,
                         ExecutorService rollupReadExecutor,
                         ThreadPoolExecutor rollupWriteExecutor,
                         ExecutorService enumValidatorExecutor,
                         AstyanaxReader astyanaxReader) {
        super(scheduleCtx, destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor, astyanaxReader);
    }

    public enum MethodToken {
        finishExecution,
        processLocator,
        processHistogramForLocator,
        executeHistogramRollupForLocator,
        executeRollupForLocator,
        getLocators,
        waitForRollups,
    };

    public class Interaction {
        public final MethodToken methodCalled;
        public final List<Object> arguments;

        public Interaction(MethodToken methodCalled, Object... arguments) {
            this.methodCalled = methodCalled;
            this.arguments = Collections.unmodifiableList(Arrays.asList(arguments));
        }
    }

    List<Interaction> interactions = new ArrayList<Interaction>();
    List<Interaction> readOnlyInteractions = Collections.unmodifiableList(interactions);
    List<Interaction> getInteractions() {
        return readOnlyInteractions;
    }

    public synchronized boolean wasMethodCalled(MethodToken method) {
        for (Interaction i : getInteractions()) {
            if (i.methodCalled == method) {
                return true;
            }
        }

        return false;
    }

    protected synchronized void recordInteraction(MethodToken methodCalled,
                                                  Object... arguments) {
        interactions.add(new Interaction(methodCalled, arguments));
    }

    @Override
    public void finishExecution(long waitStart,
                                RollupExecutionContext executionContext) {
        recordInteraction(MethodToken.finishExecution, waitStart,
                executionContext);
    }

    @Override
    public int processLocator(int rollCount,
                              RollupExecutionContext executionContext,
                              RollupBatchWriter rollupBatchWriter,
                              Locator locator) {
        recordInteraction(MethodToken.processLocator, rollCount,
                executionContext, rollupBatchWriter, locator);
        return 0;
    }

    @Override
    public int processHistogramForLocator(int rollCount,
                                          RollupExecutionContext executionContext,
                                          RollupBatchWriter rollupBatchWriter,
                                          Locator locator) {
        recordInteraction(MethodToken.processHistogramForLocator, rollCount,
                executionContext, rollupBatchWriter, locator);
        return 0;
    }

    @Override
    public void executeHistogramRollupForLocator(RollupExecutionContext executionContext,
                                                 RollupBatchWriter rollupBatchWriter,
                                                 Locator locator) {
        recordInteraction(MethodToken.executeHistogramRollupForLocator,
                executionContext, rollupBatchWriter, locator);
    }

    @Override
    public void executeRollupForLocator(RollupExecutionContext executionContext,
                                        RollupBatchWriter rollupBatchWriter,
                                        Locator locator) {
        recordInteraction(MethodToken.executeRollupForLocator, executionContext,
                rollupBatchWriter, locator);
    }

    @Override
    public Set<Locator> getLocators(RollupExecutionContext executionContext) {
        recordInteraction(MethodToken.getLocators, executionContext);
        return new HashSet<Locator>();
    }

    boolean throwsInterruptedException = false;
    public void setThrowsInterruptedException(boolean value) {
        throwsInterruptedException = value;
    }
    @Override
    public void waitForRollups() throws InterruptedException {
        recordInteraction(MethodToken.waitForRollups);

        if (throwsInterruptedException) {
            throw new InterruptedException("exception for testing purposes");
        }
    }
}
