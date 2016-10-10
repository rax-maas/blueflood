package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.*;

public class LocatorFetchRunnableDrainExecutionContextTest {


    ScheduleContext scheduleCtx;
    SlotKey destSlotKey;
    ExecutorService rollupReadExecutor;
    ThreadPoolExecutor rollupWriteExecutor;
    ExecutorService enumValidatorExecutor;

    LocatorFetchRunnable lfr;

    RollupExecutionContext executionContext;
    RollupBatchWriter rollupBatchWriter;

    List<Locator> locators;

    @Before
    public void setUp() throws IOException {

        Configuration.getInstance().init();

        this.scheduleCtx = mock(ScheduleContext.class);
        this.destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        this.rollupReadExecutor = mock(ExecutorService.class);
        this.rollupWriteExecutor = mock(ThreadPoolExecutor.class);
        this.enumValidatorExecutor = mock(ExecutorService.class);

        this.lfr = mock(LocatorFetchRunnable.class);
        this.lfr.initialize(scheduleCtx, destSlotKey,
                rollupReadExecutor, rollupWriteExecutor, enumValidatorExecutor);
        doCallRealMethod().when(lfr).drainExecutionContext(
                anyLong(), anyInt(), Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());

        executionContext = mock(RollupExecutionContext.class);
        rollupBatchWriter = mock(RollupBatchWriter.class);

        locators = getTypicalLocators();
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
        verifyZeroInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verify(lfr).initialize(
                Matchers.<ScheduleContext>any(),
                Matchers.<SlotKey>any(),
                Matchers.<ExecutorService>any(),
                Matchers.<ThreadPoolExecutor>any(),
                Matchers.<ExecutorService>any()
        );
        verify(lfr).drainExecutionContext(anyLong(), anyInt(),
                Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());
        verify(lfr).finishExecution(anyLong(), Matchers.<RollupExecutionContext>any());
        verifyNoMoreInteractions(lfr);
    }

    @Test
    public void drainExecutionContextWaitsForRollups() throws InterruptedException {

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
        verifyZeroInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verify(lfr).initialize(
                Matchers.<ScheduleContext>any(),
                Matchers.<SlotKey>any(),
                Matchers.<ExecutorService>any(),
                Matchers.<ThreadPoolExecutor>any(),
                Matchers.<ExecutorService>any()
        );
        verify(lfr).drainExecutionContext(anyLong(), anyInt(),
                Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());
        verify(lfr).finishExecution(anyLong(), Matchers.<RollupExecutionContext>any());
        verify(lfr).waitForRollups();
        verifyNoMoreInteractions(lfr);
    }

    @Test
    public void drainExecutionContextDoneReadingInFinallyBlock() throws InterruptedException {

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
        verifyZeroInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verify(lfr).initialize(
                Matchers.<ScheduleContext>any(),
                Matchers.<SlotKey>any(),
                Matchers.<ExecutorService>any(),
                Matchers.<ThreadPoolExecutor>any(),
                Matchers.<ExecutorService>any()
        );
        verify(lfr).drainExecutionContext(anyLong(), anyInt(),
                Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());
        verify(lfr).finishExecution(anyLong(), Matchers.<RollupExecutionContext>any());
        verify(lfr).waitForRollups();
        verifyNoMoreInteractions(lfr);
    }

    @Test
    public void drainExecutionContextExceptionWhileWaitingHitsExceptionHandler() throws InterruptedException {

        // given
        Throwable cause = new InterruptedException("exception for testing purposes");
        doThrow(cause).when(lfr).waitForRollups();
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
        verifyZeroInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verify(lfr).initialize(
                Matchers.<ScheduleContext>any(),
                Matchers.<SlotKey>any(),
                Matchers.<ExecutorService>any(),
                Matchers.<ThreadPoolExecutor>any(),
                Matchers.<ExecutorService>any()
        );
        verify(lfr).drainExecutionContext(anyLong(), anyInt(),
                Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());
        verify(lfr).finishExecution(anyLong(), Matchers.<RollupExecutionContext>any());
        verify(lfr).waitForRollups();
        verifyNoMoreInteractions(lfr);
    }

    @Test
    public void drainExecutionContextWhenDoneReadingDrainsBatch() throws InterruptedException {

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
        verifyZeroInteractions(scheduleCtx);
        verifyZeroInteractions(rollupReadExecutor);
        verifyZeroInteractions(rollupWriteExecutor);
        verifyZeroInteractions(enumValidatorExecutor);
        verify(lfr).initialize(
                Matchers.<ScheduleContext>any(),
                Matchers.<SlotKey>any(),
                Matchers.<ExecutorService>any(),
                Matchers.<ThreadPoolExecutor>any(),
                Matchers.<ExecutorService>any()
        );
        verify(lfr).drainExecutionContext(anyLong(), anyInt(),
                Matchers.<RollupExecutionContext>any(),
                Matchers.<RollupBatchWriter>any());
        verify(lfr).finishExecution(anyLong(), Matchers.<RollupExecutionContext>any());
        verify(lfr).waitForRollups();
        verifyNoMoreInteractions(lfr);
    }
}
