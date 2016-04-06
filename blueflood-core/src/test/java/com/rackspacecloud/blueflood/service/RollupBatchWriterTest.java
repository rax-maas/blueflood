package com.rackspacecloud.blueflood.service;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.*;

public class RollupBatchWriterTest {

    ThreadPoolExecutor executor;
    RollupExecutionContext ctx;

    RollupBatchWriter rbw;

    @Before
    public void setUp() {

        executor = mock(ThreadPoolExecutor.class);
        ctx = mock(RollupExecutionContext.class);
        rbw = new RollupBatchWriter(executor, ctx);
    }

    @Test
    public void enqueueIncrementsWriterCounter() {

        // given
        SingleRollupWriteContext srwc = mock(SingleRollupWriteContext.class);

        // when
        rbw.enqueueRollupForWrite(srwc);

        // then
        verify(ctx).incrementWriteCounter();
        verifyNoMoreInteractions(ctx);

        verifyZeroInteractions(srwc);
    }

    @Test
    public void enqueuingLessThanMinSizeDoesNotTriggerBatching() {

        // given
        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        // when
        rbw.enqueueRollupForWrite(srwc1);
        rbw.enqueueRollupForWrite(srwc2);
        rbw.enqueueRollupForWrite(srwc3);
        rbw.enqueueRollupForWrite(srwc4);

        // then
        verify(ctx, times(4)).incrementWriteCounter();
        verifyNoMoreInteractions(ctx);

        verifyZeroInteractions(srwc1);
        verifyZeroInteractions(srwc2);
        verifyZeroInteractions(srwc3);
        verifyZeroInteractions(srwc4);

        verifyZeroInteractions(executor);
    }

    @Test
    public void enqueuingMinSizeTriggersCheckOnExecutor() {

        // given

        // if active count == pool size, the RollupBatchWriter will think the
        // thread pool is saturated, and not drain
        doReturn(1).when(executor).getActiveCount();
        doReturn(1).when(executor).getPoolSize();

        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc5 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        // when
        rbw.enqueueRollupForWrite(srwc1);
        rbw.enqueueRollupForWrite(srwc2);
        rbw.enqueueRollupForWrite(srwc3);
        rbw.enqueueRollupForWrite(srwc4);
        rbw.enqueueRollupForWrite(srwc5);

        // then
        verify(ctx, times(5)).incrementWriteCounter();
        verifyNoMoreInteractions(ctx);

        verifyZeroInteractions(srwc1);
        verifyZeroInteractions(srwc2);
        verifyZeroInteractions(srwc3);
        verifyZeroInteractions(srwc4);
        verifyZeroInteractions(srwc5);

        // if the queue size >= min size, then the executor will be queried
        verify(executor).getActiveCount();
        verify(executor).getPoolSize();
        verifyNoMoreInteractions(executor);
    }

    @Test
    public void enqueuingMinSizeAndThreadPoolNotSaturatedTriggersBatching() {

        // given
        // if active count < pool size, the RollupBatchWriter will think the
        // thread pool is NOT saturated, and start batching
        doReturn(0).when(executor).getActiveCount();
        doReturn(1).when(executor).getPoolSize();

        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc5 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        // when
        rbw.enqueueRollupForWrite(srwc1);
        rbw.enqueueRollupForWrite(srwc2);
        rbw.enqueueRollupForWrite(srwc3);
        rbw.enqueueRollupForWrite(srwc4);
        rbw.enqueueRollupForWrite(srwc5);

        // then
        verify(ctx, times(5)).incrementWriteCounter();
        verifyNoMoreInteractions(ctx);

        verifyZeroInteractions(srwc1);
        verifyZeroInteractions(srwc2);
        verifyZeroInteractions(srwc3);
        verifyZeroInteractions(srwc4);
        verifyZeroInteractions(srwc5);

        // if the queue size >= min size, then the executor will be queried
        verify(executor).getActiveCount();
        verify(executor).getPoolSize();
        verify(executor).execute(Matchers.<Runnable>any());
        verifyNoMoreInteractions(executor);
    }

    @Test
    public void enqueuingMaxSizeTriggersBatching() {

        // given
        // if active count == pool size, the RollupBatchWriter will think the
        // thread pool is saturated, and not drain
        doReturn(1).when(executor).getActiveCount();
        doReturn(1).when(executor).getPoolSize();

        SingleRollupWriteContext[] srwcs = new SingleRollupWriteContext[100];
        int i;
        for (i = 0; i < 100; i++) {
            srwcs[i] = mock(SingleRollupWriteContext.class);
        }
        // ROLLUP_BATCH_MAX_SIZE default value is 100

        // when
        for (i = 0; i < 100; i++) {
            rbw.enqueueRollupForWrite(srwcs[i]);
        }

        // then
        verify(ctx, times(100)).incrementWriteCounter();
        verifyNoMoreInteractions(ctx);

        for (i = 0; i < 100; i++) {
            verifyZeroInteractions(srwcs[i]);
        }

        // if the queue size >= min size, then the executor will be queried
        verify(executor, times(96)).getActiveCount();   // ROLLUP_BATCH_MAX_SIZE - ROLLUP_BATCH_MIN_SIZE + 1
        verify(executor, times(96)).getPoolSize();
        verify(executor).execute(Matchers.<Runnable>any());
        verifyNoMoreInteractions(executor);
    }

    @Test
    public void drainBatchWithNoItemsDoesNotTriggerBatching() {

        // when
        rbw.drainBatch();

        // then
        verifyZeroInteractions(ctx);

        verifyZeroInteractions(executor);
    }

    @Test
    public void drainBatchWithSingleItemTriggersBatching() {

        // given
        SingleRollupWriteContext srwc = mock(SingleRollupWriteContext.class);
        rbw.enqueueRollupForWrite(srwc);

        // when
        rbw.drainBatch();

        // then
        verify(ctx).incrementWriteCounter();    // this invocation due to setup
        verifyNoMoreInteractions(ctx);

        verify(executor).execute(Matchers.<Runnable>any());
        verifyNoMoreInteractions(executor);
    }
}
