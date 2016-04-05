package com.rackspacecloud.blueflood.service;

import org.junit.Test;
import org.mockito.Matchers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class RollupBatchWriterTest {

    @Test
    public void enqueueIncrementsWriterCounter() {

        // given
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(5);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 1000, TimeUnit.MILLISECONDS, queue);
        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        SingleRollupWriteContext srwc = mock(SingleRollupWriteContext.class);

        RollupBatchWriter rbw = new RollupBatchWriter(executor, ctx);

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
        ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        RollupBatchWriter rbw = new RollupBatchWriter(executor, ctx);

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
        ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
        // if active count == pool size, the RollupBatchWriter will think the
        // thread pool is saturated, and not drain
        doReturn(1).when(executor).getActiveCount();
        doReturn(1).when(executor).getPoolSize();

        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc5 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        RollupBatchWriter rbw = new RollupBatchWriter(executor, ctx);

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
        ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
        // if active count < pool size, the RollupBatchWriter will think the
        // thread pool is NOT saturated, and start batching
        doReturn(0).when(executor).getActiveCount();
        doReturn(1).when(executor).getPoolSize();

        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        SingleRollupWriteContext srwc1 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc2 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc3 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc4 = mock(SingleRollupWriteContext.class);
        SingleRollupWriteContext srwc5 = mock(SingleRollupWriteContext.class);
        // ROLLUP_BATCH_MIN_SIZE default value is 5

        RollupBatchWriter rbw = new RollupBatchWriter(executor, ctx);

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
}
