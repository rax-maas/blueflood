package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.threading.SizedExecutorService;
import com.rackspacecloud.blueflood.threading.ThreadPoolSizedExecutorServiceAdapter;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;

public class LocatorFetchRunnableTest {

    @Test
    public void finestGranularityWillQuitEarly() {

        ScheduleContext scheduleCtx = mock(ScheduleContext.class);
        SlotKey destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        ExecutorService rollupReadExecutor = mock(ExecutorService.class);
        SizedExecutorService rollupWriteExecutor = mock(SizedExecutorService.class);
        ExecutorService enumValidatorExecutor = mock(ExecutorService.class);

        LocatorFetchRunnable runnable = new LocatorFetchRunnable(scheduleCtx,
                destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor);

        runnable.run();
    }
}
