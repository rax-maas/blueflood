package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.BoundStatement;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Unit tests for DBasicMetricsRW class and its parent class DAbstractMetricsRW.
 */
public class DBasicMetricsRWTest {

    private DLocatorIO locatorIO = mock(DLocatorIO.class);
    private DDelayedLocatorIO delayedLocatorIO = mock(DDelayedLocatorIO.class);

    @Test
    public void metricCollectedADayAgo_shouldBeDelayed() throws Exception {
        Clock clock = new DefaultClockImpl();
        TimeValue aDay = new TimeValue(1, TimeUnit.DAYS);
        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, clock);
        IMetric metric = new Metric(Locator.createLocatorFromPathComponents("123456", "foo.bar"),
                                    987L, clock.now().getMillis() - aDay.toMillis(),
                                    aDay, null);
        assertTrue("a day old metric is delayed", metricsRW.isDelayed(metric));
    }

    @Test
    public void onTimeMetric_shouldNotBeDelayed() throws Exception {
        Clock clock = new DefaultClockImpl();
        TimeValue aDay = new TimeValue(1, TimeUnit.DAYS);
        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, clock);
        IMetric metric = new Metric(Locator.createLocatorFromPathComponents("123456", "foo.bar"),
                987L, clock.now().getMillis(),
                aDay, null);
        assertFalse("on time metric is not delayed", metricsRW.isDelayed(metric));
    }

    @Test
    public void delayedMetric_shouldGetBoundStatement() throws Exception {
        Clock clock = new DefaultClockImpl();
        TimeValue aDay = new TimeValue(1, TimeUnit.DAYS);

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, clock);
        Locator locator = Locator.createLocatorFromPathComponents("123456", "foo.bar");
        IMetric metric = new Metric(locator,
                987L, clock.now().getMillis() - aDay.toMillis(),
                aDay, null);
        when(delayedLocatorIO.getBoundStatementForLocator(any(), anyInt(), eq(locator))).thenReturn(mock(BoundStatement.class));
        assertNotNull("delayed metric should get a boundStatement", metricsRW.getBoundStatementForMetricIfDelayed(metric));
    }

    @Test
    public void onTimeMetric_shouldNotGetBoundStatement() throws Exception {
        Clock clock = new DefaultClockImpl();
        TimeValue aDay = new TimeValue(1, TimeUnit.DAYS);

        DBasicMetricsRW metricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, clock);
        IMetric metric = new Metric(Locator.createLocatorFromPathComponents("123456", "foo.bar"),
                987L, clock.now().getMillis(),
                aDay, null);
        assertNull("on time metric should NOT get a boundStatement", metricsRW.getBoundStatementForMetricIfDelayed(metric));
    }
}
