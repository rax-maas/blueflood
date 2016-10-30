package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@PowerMockIgnore({"javax.management.*", "com.rackspacecloud.blueflood.utils.Metrics", "com.codahale.metrics.*"})
@PrepareForTest({ IOContainer.class })
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor( { "com.rackspacecloud.blueflood.io.IOContainer", "com.rackspacecloud.blueflood.service.RollupRunnable" } )
public class LocatorFetchRunnableTest {


    ScheduleContext scheduleCtx;
    SlotKey destSlotKey;
    ExecutorService rollupReadExecutor;
    ThreadPoolExecutor rollupWriteExecutor;
    ExecutorService enumValidatorExecutor;
    LocatorIO locatorIO;

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

        this.lfr = new LocatorFetchRunnable(scheduleCtx,
                destSlotKey, rollupReadExecutor, rollupWriteExecutor,
                enumValidatorExecutor);

        executionContext = mock(RollupExecutionContext.class);
        rollupBatchWriter = mock(RollupBatchWriter.class);

        locators = getTypicalLocators();

        // mock IOContainer and LocatorIO
        locatorIO = mock(LocatorIO.class);
        PowerMockito.mockStatic(IOContainer.class);
        IOContainer ioContainer = mock(IOContainer.class);
        when(IOContainer.fromConfig()).thenReturn(ioContainer);
        when(ioContainer.getLocatorIO()).thenReturn(locatorIO);
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
    public void getLocatorsReturnsLocators() throws IOException {

        // given
        Set<Locator> expected = new HashSet<Locator>(locators);

        when(locatorIO.getLocators(0)).thenReturn(locators);

        // when
        Set<Locator> actual = lfr.getLocators(executionContext);

        // then
        verify(locatorIO, times(1)).getLocators(0);
        verifyNoMoreInteractions(locatorIO);
        verifyZeroInteractions(executionContext);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getLocatorsExceptionYieldsEmptySet() throws IOException {

        // given
        when(locatorIO.getLocators(0)).thenThrow(new RuntimeException(""));

        // when
        Set<Locator> actual = lfr.getLocators(executionContext);

        // then
        verify(locatorIO, times(1)).getLocators(0);
        verifyNoMoreInteractions(locatorIO);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verifyNoMoreInteractions(executionContext);
        assertNotNull(actual);
        Assert.assertEquals(0, actual.size());
    }

    @Test
    public void executeRollupForLocatorTriggersExecutionOfRollupRunnable() {

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
    public void processLocatorTriggersRunnable() {

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(1, count);
        verify(executionContext, never()).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, never()).decrementReadCounter();
    }

    @Test
    public void processLocatorIncrementsCount() {

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
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause).when(rollupReadExecutor).execute(Matchers.<Runnable>any());

        // when
        int count = lfr.processLocator(0, executionContext, rollupBatchWriter, locators.get(0));

        // then
        Assert.assertEquals(0, count);
        verify(executionContext, times(1)).markUnsuccessful(Matchers.<Throwable>any());
        verify(executionContext, times(1)).decrementReadCounter();
    }

    @Test
    public void finishExecutionWhenSuccessful() {

        // given
        when(executionContext.wasSuccessful()).thenReturn(true);

        // when
        lfr.finishExecution(0, executionContext);

        // then
        verify(executionContext, times(1)).wasSuccessful();
        verifyNoMoreInteractions(executionContext);
        verify(scheduleCtx, times(1)).clearFromRunning(Matchers.<SlotKey>any());
        verify(scheduleCtx).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
    }

    @Test
    public void finishExecutionWhenNotSuccessful() {

        // given
        when(executionContext.wasSuccessful()).thenReturn(false);

        // when
        lfr.finishExecution(0, executionContext);

        // then
        verify(executionContext, times(1)).wasSuccessful();
        verifyNoMoreInteractions(executionContext);
        verify(scheduleCtx, times(1)).pushBackToScheduled(Matchers.<SlotKey>any(), eq(false));
        verify(scheduleCtx).getCurrentTimeMillis();
        verifyNoMoreInteractions(scheduleCtx);
    }

    @Test
    public void createRollupExecutionContextReturnsValidObject() {
        // when
        RollupExecutionContext execCtx = lfr.createRollupExecutionContext();

        // then
        assertNotNull(execCtx);
    }

    @Test
    public void createRollupBatchWriterReturnsValidObject() {
        // given
        RollupExecutionContext execCtx = lfr.createRollupExecutionContext();

        // when
        RollupBatchWriter batchWriter = lfr.createRollupBatchWriter(execCtx);

        //then
        assertNotNull(batchWriter);
    }
}
