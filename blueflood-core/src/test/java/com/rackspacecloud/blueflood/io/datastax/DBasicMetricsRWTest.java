package com.rackspacecloud.blueflood.io.datastax;

import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.io.MetricsRW;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;

public class DBasicMetricsRWTest {

    @Test
    public void testIsDelayedMetric() {

        final Instant currentTime = new DefaultClockImpl().now();
        final long collectionTime = currentTime.getMillis() - 5;
        final Metric metric = createTestMetric(collectionTime);

        final Clock mockClock = mock(Clock.class);
        DBasicMetricsRW dbasicMetricsRW = new DBasicMetricsRW(mockClock);

        when(mockClock.now()).thenReturn(currentTime);
        assertEquals("Should not be a delayed metric", false, dbasicMetricsRW.isDelayedMetric(metric));

        //setting current time in future
        final Instant futureTime = new Instant(currentTime.getMillis() +
                Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS) + 1);

        when(mockClock.now()).thenReturn(futureTime);
        assertEquals("Should be a delayed metric", true, dbasicMetricsRW.isDelayedMetric(metric));
    }

    @Test
    public void testInsertMetricsLocatorCache() throws IOException {

        final Instant currentTime = new DefaultClockImpl().now();
        final long collectionTime = currentTime.getMillis() - 5;
        final Metric metric = createTestMetric(collectionTime);

        final Clock mockClock = mock(Clock.class);
        LocatorIO mockLocatorIO = mock(LocatorIO.class);
        MetricsRW metricsRW = new DBasicMetricsRW(mockClock, mockLocatorIO);

        when(mockClock.now()).thenReturn(currentTime);
        doNothing().when(mockLocatorIO).insertLocator(any(Locator.class));

        //inserting a locator 3 times
        for (int i = 0; i < 3 ; i ++) {
            metricsRW.insertMetrics(new ArrayList<IMetric>() {{
                add(metric);
            }});
        }

        //Should insert locator only once
        verify(mockLocatorIO, times(1)).insertLocator(any(Locator.class));
    }

    @Test
    public void testInsertMetricsForDelayedMetrics() throws IOException {

        final Instant currentTime = new DefaultClockImpl().now();
        final long collectionTime = currentTime.getMillis() - 5;
        final Metric metric = createTestMetric(collectionTime);

        final Clock mockClock = mock(Clock.class);
        LocatorIO mockLocatorIO = mock(LocatorIO.class);
        MetricsRW metricsRW = new DBasicMetricsRW(mockClock, mockLocatorIO);

        when(mockClock.now()).thenReturn(currentTime);
        doNothing().when(mockLocatorIO).insertLocator(any(Locator.class));

        metricsRW.insertMetrics(new ArrayList<IMetric>() {{
            add(metric);
        }});

        //setting current time in future
        final Instant futureTime = new Instant(currentTime.getMillis() +
                Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS) + 1);

        when(mockClock.now()).thenReturn(futureTime);

        //inserting a locator 3 times
        for (int i = 0; i < 3 ; i ++) {
            metricsRW.insertMetrics(new ArrayList<IMetric>() {{
                add(metric);
            }});
        }

        //Should insert locator only once
        verify(mockLocatorIO, times(4)).insertLocator(any(Locator.class));
    }

    private Metric createTestMetric(long collectionTime) {
        Locator locator = Locator.createLocatorFromPathComponents("99999",
                DBasicMetricsRWTest.class.getName() + ".numeric.metric." + System.currentTimeMillis());
        return new Metric( locator, 1, collectionTime, new TimeValue(1, TimeUnit.DAYS), "unit");
    }
}
