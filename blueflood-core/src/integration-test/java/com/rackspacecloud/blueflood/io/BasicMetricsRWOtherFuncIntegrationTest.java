package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.datastax.DBasicMetricsRW;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.Util;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test non-read/write functionality of the datstax driver.
 */
@PowerMockIgnore({"javax.management.*" })
@RunWith( PowerMockRunner.class )
@PowerMockRunnerDelegate( JUnitParamsRunner.class )
@PrepareForTest( DBasicMetricsRW.class )
public class BasicMetricsRWOtherFuncIntegrationTest extends BasicMetricsRWIntegrationTest {

    @Test
    @Parameters(method="getGranularitiesToTest")
    public void testSingleMetricTtlWorks(Granularity granularity) throws Exception {

        // pick first locator from input metrics
        Locator locator = numericMap.keySet().iterator().next();
        IMetric expectedMetric = numericMap.get(locator);

        // put it, with TTL 2 seconds
        expectedMetric.setTtlInSeconds(2);

        List<SingleRollupWriteContext> cxts = new ArrayList<SingleRollupWriteContext>();
        cxts.add( createSingleRollupWriteContext( granularity, expectedMetric ));

        // mock getTtl() to return 2 seconds
        MetricsRW mockDatastaxMetricsRW = spy( datastaxMetricsRW );
        doReturn( 2 ).when( mockDatastaxMetricsRW, "getTtl", anyObject(), anyObject(), anyObject() );

        mockDatastaxMetricsRW.insertRollups( cxts );

        // read it quickly.
        Points<BluefloodTimerRollup> points =
                datastaxMetricsRW.getDataToRollup(locator,
                        expectedMetric.getRollupType(),
                        getRangeFromMinAgoToNow(5),
                        CassandraModel.getBasicColumnFamilyName(granularity));
        assertEquals("number of points read before TTL", 1, points.getPoints().size());

        // let it time out.
        Thread.sleep(2000);

        // ensure it is gone.
        points = datastaxMetricsRW.getDataToRollup(locator,
                expectedMetric.getRollupType(),
                getRangeFromMinAgoToNow(5),
                CassandraModel.getBasicColumnFamilyName(granularity));
        assertEquals( "number of points read after TTL", 0, points.getPoints().size() );
    }

    @Test
    public void testHigherGranReadWrite() throws Exception {

        // pick first locator from input metrics
        Locator locator = numericMap.keySet().iterator().next();
        final IMetric expectedMetric = numericMap.get(locator);

        // pick a granularity
        Granularity granularity = Granularity.MIN_60;

        List<SingleRollupWriteContext> cxts = new ArrayList<SingleRollupWriteContext>();
        cxts.add( createSingleRollupWriteContext( granularity, expectedMetric ));

        // insert metric
        datastaxMetricsRW.insertRollups(cxts);

        // read the raw data.
        Points points =
                datastaxMetricsRW.getDataToRollup( expectedMetric.getLocator(),
                        expectedMetric.getRollupType(),
                        getRangeFromMinAgoToNow( 5 ),
                        CassandraModel.getBasicColumnFamilyName( granularity ) );
        assertEquals("number of points read", 1, points.getPoints().size());

        // create the rollup
        final BasicRollup rollup = BasicRollup.buildRollupFromRollups(points);
        // should be the same as simpletimerRollup   Assert.assertEquals(timerRollup, rollup);

        // assemble it into points, but give it a new timestamp.
        points = new Points<BasicRollup>() {{
            add(new Point(expectedMetric.getCollectionTime(), rollup));
        }};

        List<SingleRollupWriteContext> coaserCxts = toWriteContext(expectedMetric.getLocator(), points, granularity.coarser());

        datastaxMetricsRW.insertRollups(coaserCxts);

        // we should be able to read that now.
        Points<BasicRollup> pointsCoarser =
                datastaxMetricsRW.getDataToRollup(
                        expectedMetric.getLocator(),
                        RollupType.BF_BASIC,
                        getRangeFromMinAgoToNow( 5 ),
                        CassandraModel.getBasicColumnFamilyName( granularity.coarser() ) );

        assertEquals("number of points read in coarser gran", 1, pointsCoarser.getPoints().size());

        BasicRollup rollupCoarser = pointsCoarser.getPoints().values().iterator().next().getData();
        // rollups should be identical since one is just a coarse rollup of the other.
        assertEquals( "rollup read in coarser gran is the same", rollup, rollupCoarser );
    }


    @Test
    public void testLocatorWritten() throws Exception {

        // insert metrics using datastax
        datastaxMetricsRW.insertMetrics(numericMap.values());

        LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();
        for ( Locator locator : numericMap.keySet() ) {
            long shard = Util.getShard( locator.toString() );
            Collection<Locator> locators = locatorIO.getLocators(shard);
            assertTrue(String.format("locator %s should exist", locator), locators.contains(locator));

            for (Locator locatorFromDB: locators) {
                assertTrue("Locator should have last update time information", locatorFromDB.getLastUpdatedTimestamp() > 0);
            }
        }
    }

    @Test
    public void testLocatorWrittenOnlyOnce() throws Exception {

        Clock mockClock = mock(Clock.class);
        MetricsRW dsMetricsRW = new DBasicMetricsRW(mockClock);
        when(mockClock.now()).thenReturn(new DefaultClockImpl().now());

        // insert metrics using datastax
        dsMetricsRW.insertMetrics(numericMap.values());

        Locator locator = new ArrayList<Locator>(numericMap.keySet()).get(0);
        long shard = Util.getShard(locator.toString());
        LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

        long locatorLastUpdateTime = getLocatorLastUpdateTime(locator, shard, locatorIO);
        assertTrue("Locator should have last update time information", locatorLastUpdateTime > 0);

        Thread.sleep(100);
        dsMetricsRW.insertMetrics(numericMap.values());

        long locatorLastUpdateTime1 = getLocatorLastUpdateTime(locator, shard, locatorIO);
        assertTrue("Locator should have last update time information", locatorLastUpdateTime1 > 0);
        assertTrue("Locator should should not be updated again", locatorLastUpdateTime1 == locatorLastUpdateTime);
    }

    @Test
    public void testLocatorWrittenForDelayedMetrics() throws Exception {

        final Instant currentTime = new DefaultClockImpl().now();
        Clock mockClock = mock(Clock.class);
        MetricsRW dsMetricsRW = new DBasicMetricsRW(mockClock);
        when(mockClock.now()).thenReturn(currentTime);

        // insert metrics using datastax
        dsMetricsRW.insertMetrics(numericMap.values());

        Locator locator = new ArrayList<Locator>(numericMap.keySet()).get(0);
        long shard = Util.getShard(locator.toString());
        LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

        long locatorLastUpdateTime = getLocatorLastUpdateTime(locator, shard, locatorIO);
        assertTrue("Locator should have last update time information", locatorLastUpdateTime > 0);

        //inserting delayed metric
        Thread.sleep(100);
        //setting current time in future
        final Instant futureTime = new Instant(currentTime.getMillis() +
                Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS) + 1);
        when(mockClock.now()).thenReturn(futureTime);
        dsMetricsRW.insertMetrics(numericMap.values());

        long locatorLastUpdateTime1 = getLocatorLastUpdateTime(locator, shard, locatorIO);
        assertTrue("Locator should have last update time information", locatorLastUpdateTime1 > 0);
        assertTrue("Locator should should be updated again", locatorLastUpdateTime1 > locatorLastUpdateTime);
    }

    private long getLocatorLastUpdateTime(Locator locator, long shard, LocatorIO locatorIO) throws IOException {
        Collection<Locator> locators = locatorIO.getLocators(shard);
        long locatorLastUpdateTime = 0;
        for (Locator locatorFromDB: locators) {
            if (locatorFromDB.equals(locator)) {
                locatorLastUpdateTime = locatorFromDB.getLastUpdatedTimestamp();
                break;
            }
        }
        return locatorLastUpdateTime;
    }


}
