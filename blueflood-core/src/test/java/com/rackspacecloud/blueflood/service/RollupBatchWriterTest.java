package com.rackspacecloud.blueflood.service;

import org.junit.Test;

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
}
