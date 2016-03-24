package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.threading.SizedExecutorService;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class LocatorFetchRunnableDrainExecutionContextTest {


    ScheduleContext scheduleCtx;
    SlotKey destSlotKey;
    ExecutorService rollupReadExecutor;
    SizedExecutorService rollupWriteExecutor;
    ExecutorService enumValidatorExecutor;
    AstyanaxReader astyanaxReader;

    StubbedLocatorFetchRunnable lfr;

    RollupExecutionContext executionContext;
    RollupBatchWriter rollupBatchWriter;

    List<Locator> locators;

    @Before
    public void setUp() throws IOException {

        Configuration.getInstance().init();

        this.scheduleCtx = mock(ScheduleContext.class);
        this.destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        this.rollupReadExecutor = mock(ExecutorService.class);
        this.rollupWriteExecutor = mock(SizedExecutorService.class);
        this.enumValidatorExecutor = mock(ExecutorService.class);
        this.astyanaxReader = mock(AstyanaxReader.class);

        this.lfr = new StubbedLocatorFetchRunnable(scheduleCtx, destSlotKey,
                rollupReadExecutor, rollupWriteExecutor, enumValidatorExecutor,
                astyanaxReader);

        executionContext = mock(RollupExecutionContext.class);
        rollupBatchWriter = mock(RollupBatchWriter.class);

        locators = getTypicalLocators();

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS,
                "false");
    }

    @After
    public void tearDown() throws IOException {
        Configuration.getInstance().init();
    }

    List<Locator> getTypicalLocators() {

        final Locator locator1 =
                Locator.createLocatorFromPathComponents("tenant1", "a", "b", "c");
        final Locator locator2 =
                Locator.createLocatorFromPathComponents("tenant2", "a", "b", "x");
        final Locator locator3 =
                Locator.createLocatorFromPathComponents("tenant3", "d", "e", "f");
        final List<Locator> locators = new ArrayList<Locator>() {{
            add(locator1);
            add(locator2);
            add(locator3);
        }};
        return locators;
    }

    @Test
    public void drainExecutionContextAlreadyDoneReadingAndWriting() {

        // given
        when(executionContext.doneReading()).thenReturn(true);
        when(executionContext.doneWriting()).thenReturn(true);

        // when
        lfr.drainExecutionContext(0, 0, executionContext, rollupBatchWriter);

        // then
        verify(executionContext, times(1)).doneReading();
        verify(executionContext, times(1)).doneWriting();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
        verify(scheduleCtx, times(1)).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verifyZeroInteractions(astyanaxReader);
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.finishExecution)); // verify(lfr, times(1)).finishExecution(...)
        assertEquals(1, lfr.getInteractions().size());  // verifyNoMoreInteractions(lfr)
    }

    @Test
    public void drainExecutionContextWaitsForRollups() {

        // given
        when(executionContext.doneReading())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);
        when(executionContext.doneWriting()).thenReturn(true);

        // when
        lfr.drainExecutionContext(0, 0, executionContext, rollupBatchWriter);

        // then
        verify(executionContext, times(4)).doneReading();
        verify(executionContext, times(1)).doneWriting();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
        verify(scheduleCtx, times(1)).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verifyZeroInteractions(astyanaxReader);
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.finishExecution)); // verify(lfr, times(1)).finishExecution(...)
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.waitForRollups)); // verify(lfr, times(1)).waitForRollups(...)
        assertEquals(2, lfr.getInteractions().size());  // verifyNoMoreInteractions(lfr)
    }

    @Test
    public void drainExecutionContextDoneReadingInFinallyBlock() {

        // given
        when(executionContext.doneReading())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(true);
        when(executionContext.doneWriting()).thenReturn(true);

        // when
        lfr.drainExecutionContext(0, 0, executionContext, rollupBatchWriter);

        // then
        verify(executionContext, times(4)).doneReading();
        verify(executionContext, times(1)).doneWriting();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
        verify(scheduleCtx, times(1)).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verifyZeroInteractions(astyanaxReader);
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.finishExecution)); // verify(lfr, times(1)).finishExecution(...)
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.waitForRollups)); // verify(lfr, times(1)).waitForRollups(...)
        assertEquals(2, lfr.getInteractions().size());  // verifyNoMoreInteractions(lfr)
    }

    @Test
    public void drainExecutionContextExceptionWhileWaitingHitsExceptionHandler() {

        // given
        lfr.setThrowsInterruptedException(true);
        when(executionContext.doneReading())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(true);
        when(executionContext.doneWriting()).thenReturn(true);

        // when
        lfr.drainExecutionContext(0, 0, executionContext, rollupBatchWriter);

        // then
        verify(executionContext, times(4)).doneReading();
        verify(executionContext, times(1)).doneWriting();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
        verify(scheduleCtx, times(1)).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verifyZeroInteractions(astyanaxReader);
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.finishExecution)); // verify(lfr, times(1)).finishExecution(...)
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.waitForRollups)); // verify(lfr, times(1)).waitForRollups(...)
        assertEquals(2, lfr.getInteractions().size());  // verifyNoMoreInteractions(lfr)
    }

    @Test
    public void drainExecutionContextWhenDoneReadingDrainsBatch() {

        // given
        when(executionContext.doneReading()).thenReturn(true);
        when(executionContext.doneWriting())
                .thenReturn(false)
                .thenReturn(true);

        // when
        lfr.drainExecutionContext(0, 0, executionContext, rollupBatchWriter);

        // then
        verify(executionContext, times(4)).doneReading();
        verify(executionContext, times(2)).doneWriting();
        verifyNoMoreInteractions(executionContext);
        verify(rollupBatchWriter).drainBatch();
        verifyNoMoreInteractions(rollupBatchWriter);
        verify(scheduleCtx, times(1)).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verifyZeroInteractions(astyanaxReader);
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.finishExecution)); // verify(lfr, times(1)).finishExecution(...)
        assertTrue(lfr.wasMethodCalled(StubbedLocatorFetchRunnable.MethodToken.waitForRollups)); // verify(lfr, times(1)).waitForRollups(...)
        assertEquals(2, lfr.getInteractions().size());  // verifyNoMoreInteractions(lfr)
    }
}
