package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class RollupServiceTest {

    ScheduleContext context;
    ShardStateManager shardStateManager;
    ThreadPoolExecutor locatorFetchExecutors;
    ThreadPoolExecutor rollupReadExecutors;
    ThreadPoolExecutor rollupWriteExecutors;
    ThreadPoolExecutor enumValidatorExecutor;
    long rollupDelayMillis;
    long delayedMetricRollupDelayMillis;
    long pollerPeriod;
    long configRefreshInterval;

    RollupService service;

    @Before
    public void setUp() {

        context = mock(ScheduleContext.class);
        shardStateManager = mock(ShardStateManager.class);
        locatorFetchExecutors = mock(ThreadPoolExecutor.class);
        rollupReadExecutors = mock(ThreadPoolExecutor.class);
        rollupWriteExecutors = mock(ThreadPoolExecutor.class);
        enumValidatorExecutor = mock(ThreadPoolExecutor.class);
        rollupDelayMillis = 300000;
        delayedMetricRollupDelayMillis = 300000;
        pollerPeriod = 60000;
        configRefreshInterval = 10000;

        service = new RollupService(context, shardStateManager,
                locatorFetchExecutors, rollupReadExecutors,
                rollupWriteExecutors, enumValidatorExecutor, rollupDelayMillis,
                delayedMetricRollupDelayMillis, pollerPeriod,
                configRefreshInterval);
    }

    @Test
    public void pollSchedulesEligibleSlots() {

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

    @Test
    public void runSingleSlotDequeuesAndExecutes() {

        // given
        when(context.hasScheduled()).thenReturn(true).thenReturn(false);

        SlotKey slotkey = SlotKey.of(Granularity.MIN_20, 2, 0);
        doReturn(slotkey).when(context).getNextScheduled();

        final Runnable[] capture = new Runnable[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                capture[0] = (Runnable)invocationOnMock.getArguments()[0];

                // now that the runnable has been queue, stop the outer loop
                service.setShouldKeepRunning(false);

                return null;
            }
        }).when(locatorFetchExecutors).execute(Matchers.<Runnable>any());


        // when
        service.run();

        // then
        verify(context).scheduleEligibleSlots(anyLong(), anyLong());  // from poll
        verify(context, times(2)).hasScheduled();
        verify(context).getNextScheduled();
        verify(context, times(2)).getCurrentTimeMillis();  // one from LocatorFetchRunnable ctor, one from run
        verifyNoMoreInteractions(context);

        verifyZeroInteractions(shardStateManager);

        verify(locatorFetchExecutors).execute(Matchers.<Runnable>any());
        assertSame(LocatorFetchRunnable.class, capture[0].getClass());
        verifyNoMoreInteractions(locatorFetchExecutors);

        verifyZeroInteractions(rollupReadExecutors);
        verifyZeroInteractions(rollupWriteExecutors);
        verifyZeroInteractions(enumValidatorExecutor);
    }

}
