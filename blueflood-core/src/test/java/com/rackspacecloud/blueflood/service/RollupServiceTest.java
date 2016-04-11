package com.rackspacecloud.blueflood.service;

import org.junit.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class RollupServiceTest {

    @Test
    public void pollSchedulesEligibleSlots() {

        // given
        ScheduleContext context = mock(ScheduleContext.class);
        ShardStateManager shardStateManager = mock(ShardStateManager.class);
        ThreadPoolExecutor locatorFetchExecutors = mock(ThreadPoolExecutor.class);
        ThreadPoolExecutor rollupReadExecutors = mock(ThreadPoolExecutor.class);
        ThreadPoolExecutor rollupWriteExecutors = mock(ThreadPoolExecutor.class);
        ThreadPoolExecutor enumValidatorExecutor = mock(ThreadPoolExecutor.class);
        long rollupDelayMillis = 300000;
        long delayedMetricRollupDelayMillis = 300000;

        RollupService service = new RollupService(context, shardStateManager,
                locatorFetchExecutors, rollupReadExecutors,
                rollupWriteExecutors, enumValidatorExecutor, rollupDelayMillis,
                delayedMetricRollupDelayMillis);

        // when
        service.poll();

        // then
        verify(context).scheduleEligibleSlots(anyLong(), anyLong());
        verifyNoMoreInteractions(context);

        verifyZeroInteractions(shardStateManager);
        verifyZeroInteractions(locatorFetchExecutors);
        verifyZeroInteractions(rollupReadExecutors);
        verifyZeroInteractions(rollupWriteExecutors);
        verifyZeroInteractions(enumValidatorExecutor);
    }
}
