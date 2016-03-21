package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.threading.SizedExecutorService;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.*;

public class LocatorFetchRunnableTest {

    @Test
    public void getLocatorsReturnsLocators() {

        // given
        final Locator locator1 = Locator.createLocatorFromPathComponents("tenant1", "a", "b", "c");
        final Locator locator2 = Locator.createLocatorFromPathComponents("tenant2", "a", "b", "x");
        final Locator locator3 = Locator.createLocatorFromPathComponents("tenant3", "d", "e", "f");
        final List<Locator> locators = new ArrayList<Locator>() {{
            add(locator1);
            add(locator2);
            add(locator3);
        }};
        Set<Locator> expected = new HashSet<Locator>(locators);

        AstyanaxReader astyanaxReader = mock(AstyanaxReader.class);
        when(astyanaxReader.getLocatorsToRollup(2)).thenReturn(locators);

        ScheduleContext scheduleCtx = mock(ScheduleContext.class);
        SlotKey destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        ExecutorService rollupReadExecutor = mock(ExecutorService.class);
        SizedExecutorService rollupWriteExecutor = mock(SizedExecutorService.class);
        ExecutorService enumValidatorExecutor = mock(ExecutorService.class);

        LocatorFetchRunnable lfr = new LocatorFetchRunnable(scheduleCtx,
                destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor, astyanaxReader);

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);

        // when
        Set<Locator> actual = lfr.getLocators(1, 2, executionContext);

        // then
        verify(astyanaxReader, times(1)).getLocatorsToRollup(2);
        verifyNoMoreInteractions(astyanaxReader);
        verifyZeroInteractions(executionContext);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getLocatorsExceptionYieldsEmptySet() {

        // given
        AstyanaxReader astyanaxReader = mock(AstyanaxReader.class);
        when(astyanaxReader.getLocatorsToRollup(2)).thenThrow(new RuntimeException(""));

        ScheduleContext scheduleCtx = mock(ScheduleContext.class);
        SlotKey destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        ExecutorService rollupReadExecutor = mock(ExecutorService.class);
        SizedExecutorService rollupWriteExecutor = mock(SizedExecutorService.class);
        ExecutorService enumValidatorExecutor = mock(ExecutorService.class);

        LocatorFetchRunnable lfr = new LocatorFetchRunnable(scheduleCtx,
                destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor, astyanaxReader);

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);

        // when
        Set<Locator> actual = lfr.getLocators(1, 2, executionContext);

        // then
        verify(astyanaxReader, times(1)).getLocatorsToRollup(2);
        verifyNoMoreInteractions(astyanaxReader);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verifyNoMoreInteractions(executionContext);
        Assert.assertNotNull(actual);
        Assert.assertEquals(0, actual.size());
    }
}
