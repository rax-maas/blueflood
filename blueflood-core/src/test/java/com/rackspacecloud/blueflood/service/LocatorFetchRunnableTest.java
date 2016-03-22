package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.threading.SizedExecutorService;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static org.mockito.Mockito.*;

public class LocatorFetchRunnableTest {


    ScheduleContext scheduleCtx;
    SlotKey destSlotKey;
    ExecutorService rollupReadExecutor;
    SizedExecutorService rollupWriteExecutor;
    ExecutorService enumValidatorExecutor;
    AstyanaxReader astyanaxReader;

    LocatorFetchRunnable lfr;

    @Before
    public void setUp() throws IOException {

        Configuration.getInstance().init();

        this.scheduleCtx = mock(ScheduleContext.class);
        this.destSlotKey = SlotKey.of(Granularity.FULL, 0, 0);
        this.rollupReadExecutor = mock(ExecutorService.class);
        this.rollupWriteExecutor = mock(SizedExecutorService.class);
        this.enumValidatorExecutor = mock(ExecutorService.class);
        this.astyanaxReader = mock(AstyanaxReader.class);

        this.lfr = new LocatorFetchRunnable(scheduleCtx,
                destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor, astyanaxReader);
    }

    @After
    public void tearDown() throws IOException {
        Configuration.getInstance().init();
    }

    List<Locator> getTypicalLocators() {

        final Locator locator1 = Locator.createLocatorFromPathComponents("tenant1", "a", "b", "c");
        final Locator locator2 = Locator.createLocatorFromPathComponents("tenant2", "a", "b", "x");
        final Locator locator3 = Locator.createLocatorFromPathComponents("tenant3", "d", "e", "f");
        final List<Locator> locators = new ArrayList<Locator>() {{
            add(locator1);
            add(locator2);
            add(locator3);
        }};
        return locators;
    }

    @Test
    public void getLocatorsReturnsLocators() {

        // given
        List<Locator> locators = getTypicalLocators();
        Set<Locator> expected = new HashSet<Locator>(locators);

        when(astyanaxReader.getLocatorsToRollup(0)).thenReturn(locators);

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);

        // when
        Set<Locator> actual = lfr.getLocators(executionContext);

        // then
        verify(astyanaxReader, times(1)).getLocatorsToRollup(0);
        verifyNoMoreInteractions(astyanaxReader);
        verifyZeroInteractions(executionContext);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getLocatorsExceptionYieldsEmptySet() {

        // given
        when(astyanaxReader.getLocatorsToRollup(0)).thenThrow(new RuntimeException(""));

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);

        // when
        Set<Locator> actual = lfr.getLocators(executionContext);

        // then
        verify(astyanaxReader, times(1)).getLocatorsToRollup(0);
        verifyNoMoreInteractions(astyanaxReader);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verifyNoMoreInteractions(executionContext);
        Assert.assertNotNull(actual);
        Assert.assertEquals(0, actual.size());
    }

    @Test
    public void executeRollupForLocatorTriggersExecutionOfRollupRunnable() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        // when
        lfr.executeRollupForLocator(executionContext, rollupBatchWriter, locators.get(0));

        // then
        verify(rollupReadExecutor, times(1)).execute(Matchers.<RollupRunnable>any());
        verifyNoMoreInteractions(rollupReadExecutor);
        verify(executionContext, times(1)).incrementReadCounter();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
    }

    @Test
    public void executeHistogramRollupForLocatorTriggersExecutionOfHistogramRollupRunnable() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        // when
        lfr.executeHistogramRollupForLocator(executionContext, rollupBatchWriter, locators.get(0));

        // then
        verify(rollupReadExecutor, times(1)).execute(Matchers.<HistogramRollupRunnable>any());
        verifyNoMoreInteractions(rollupReadExecutor);
        verify(executionContext, times(1)).incrementReadCounter();
        verifyNoMoreInteractions(executionContext);
        verifyZeroInteractions(rollupBatchWriter);
    }

    @Test
    public void processLocatorTriggersRunnable() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(1, count);
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
    }

    @Test
    public void processLocatorIncrementsCount() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processLocator(1, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(2, count);
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
    }

    @Test
    public void processLocatorExceptionCausesRollupToFail() {

        // given
        List<Locator> locators = getTypicalLocators();
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(0, count);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, times(1)).decrementReadCounter();
    }

    @Test
    public void processLocatorHistogramEnabledTriggersRunnables() {

        // given
        List<Locator> locators = getTypicalLocators();
        final List<RollupRunnable> executedRunnables = new ArrayList<RollupRunnable>();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                executedRunnables.add((RollupRunnable)invocation.getArguments()[0]);
                return null;
            }
        }).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "true");

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(2, count);
        verify(executionContext, times(2)).incrementReadCounter();
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
        verifyNoMoreInteractions(executionContext);
        verify(rollupReadExecutor, times(2)).execute(Matchers.<RollupRunnable>any());
        Assert.assertEquals(2, executedRunnables.size());
        Assert.assertNotNull(executedRunnables.get(0));
        Assert.assertEquals(RollupRunnable.class, executedRunnables.get(0).getClass());
        Assert.assertNotNull(executedRunnables.get(1));
        Assert.assertEquals(HistogramRollupRunnable.class, executedRunnables.get(1).getClass());
    }

    @Test
    public void processLocatorExceptionWithHistogramEnabledCausesOnlyFirstOfTwoRollupsToFail() {

        // given
        List<Locator> locators = getTypicalLocators();
        final List<RollupRunnable> executedRunnables = new ArrayList<RollupRunnable>();
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause)
            .doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    executedRunnables.add((RollupRunnable) invocation.getArguments()[0]);
                    return null;
                }
            }).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "true");

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(1, count);
        verify(executionContext, times(2)).incrementReadCounter();
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, times(1)).decrementReadCounter();
        verifyNoMoreInteractions(executionContext);
        verify(rollupReadExecutor, times(2)).execute(Matchers.<RollupRunnable>any());
        Assert.assertEquals(1, executedRunnables.size());
        Assert.assertNotNull(executedRunnables.get(0));
        Assert.assertEquals(HistogramRollupRunnable.class, executedRunnables.get(0).getClass());
    }

    @Test
    public void processHistogramForLocatorTriggersRunnable() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processHistogramForLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(1, count);
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
    }

    @Test
    public void processHistogramForLocatorIncrementsCount() {

        // given
        List<Locator> locators = getTypicalLocators();

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processHistogramForLocator(1, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(2, count);
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
    }

    @Test
    public void processHistogramForLocatorRejectedExecutionExceptionCausesRollupToFail() {

        // given
        List<Locator> locators = getTypicalLocators();
        Throwable cause = new RejectedExecutionException("exception for testing purposes");
        doThrow(cause).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processHistogramForLocator(0, executionContext,
                rollupBatchWriter, locators.get(0));    // hits the first catch block in processHistogramForLocator

        // then
        Assert.assertEquals(0, count);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, times(1)).decrementReadCounter();
    }

    @Test
    public void processHistogramForLocatorExceptionCausesRollupToFail() {

        // given
        List<Locator> locators = getTypicalLocators();
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        RollupExecutionContext executionContext = mock(RollupExecutionContext.class);
        RollupBatchWriter rollupBatchWriter = mock(RollupBatchWriter.class);

        Configuration.getInstance().setProperty(CoreConfig.ENABLE_HISTOGRAMS, "false");

        // when
        int count = lfr.processHistogramForLocator(0, executionContext,
                rollupBatchWriter, locators.get(0));    // hits the second catch block in processHistogramForLocator

        // then
        Assert.assertEquals(0, count);
        verify(executionContext, times(0)).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, times(1)).decrementReadCounter();
    }
}
