package com.rackspacecloud.blueflood.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rackspacecloud.blueflood.io.datastax.DBasicMetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Verify MetricsRW for basic metrics (SimpleNumber, String, Boolean) implementations, here when Datastax is writing and
 * Astyanax is reading.
 */
@RunWith( JUnitParamsRunner.class )
public class BasicMetricsRWDatastaxWriteIntegrationTest extends BasicMetricsRWIntegrationTest {

    @Test
    public void testNumericMultiMetricsDatapointsRangeFull() throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( numericMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator, expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            assertEquals( String.format( "locator %s data is the same", locator ),
                    ( new SimpleNumber( expectedMetric.getMetricValue() ) ), point.getData() );
        }
    }

    @Test
    public void testNumericMultiMetricsDatapointsRangeFullWithDelayedMetrics() throws IOException {

        MetricsRW datastaxMetricsRW1 = new DBasicMetricsRW(locatorIO, delayedLocatorIO, true, false, new DefaultClockImpl());

        //making one metric delayed
        final Locator delayedLocator = numericMap.keySet().iterator().next();
        IMetric currentMetric = numericMap.get(delayedLocator);
        final IMetric delayedMetric = new Metric(currentMetric.getLocator(), currentMetric.getMetricValue(),
                currentMetric.getCollectionTime() - MAX_AGE_ALLOWED - 1000, new TimeValue(currentMetric.getTtlInSeconds(),
                TimeUnit.MILLISECONDS), "unit");
        numericMap.put(delayedLocator, delayedMetric);

        // write with datastax
        datastaxMetricsRW1.insertMetrics( numericMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5 + (int) (MAX_AGE_ALLOWED / 60 / 1000)),
                Granularity.FULL );

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator, expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            assertEquals( String.format( "locator %s data is the same", locator ),
                    ( new SimpleNumber( expectedMetric.getMetricValue() ) ), point.getData() );
        }

        Set<Locator> ingestedLocators = numericMap.keySet();
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(numericMap.values()));
        locatorsFromDBByShardAndSlot.retainAll(ingestedLocators);
        assertEquals("Locators which are not delayed identified as delayed", 1, locatorsFromDBByShardAndSlot.size());
        assertEquals("Invalid delayed locator", delayedLocator.toString(), locatorsFromDBByShardAndSlot.iterator().next().toString());
    }

    @Test
    public void testNumericMultiMetricsDatapointsRangeFullWithOnlyDelayedMetrics() throws IOException {

        Clock clock = mock(Clock.class);
        final Locator locator1 = numericMap.keySet().iterator().next();
        when(clock.now()).thenReturn(new Instant(numericMap.get(locator1).getCollectionTime() + MAX_AGE_ALLOWED + 1000));

        MetricsRW datastaxMetricsRW1 = new DBasicMetricsRW(locatorIO, delayedLocatorIO, true, false, clock);

        // write with datastax
        datastaxMetricsRW1.insertMetrics( numericMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator, expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            assertEquals( String.format( "locator %s data is the same", locator ),
                    ( new SimpleNumber( expectedMetric.getMetricValue() ) ), point.getData() );
        }

        Set<Locator> ingestedLocators = numericMap.keySet();
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(numericMap.values()));
        locatorsFromDBByShardAndSlot.retainAll(ingestedLocators);
        assertEquals("Locators which are not delayed identified as delayed", locators.size(), locatorsFromDBByShardAndSlot.size());
    }

    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testNumericMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with datastax
        List<SingleRollupWriteContext> writeContexts = toWriteContext(numericMap.values(), granularity);
        datastaxMetricsRW.insertRollups(writeContexts);

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity);

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator, expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            BasicRollup rollup = (BasicRollup)pointMap.get(expectedMetric.getCollectionTime()).getData();
            assertEquals( String.format( "locator %s average is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getAverage().toLong() );
            assertEquals( String.format( "locator %s max value is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getMaxValue().toLong() );
            assertEquals( String.format( "locator %s min value is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getMinValue().toLong() );
            assertEquals( String.format( "locator %s sum is the same", locator ),
                    (Long)expectedMetric.getMetricValue(),
                    rollup.getSum(), EPSILON );
        }
    }

    @Test
    public void testNumericSingleMetricDatapointsForRangeFull() throws IOException {

        Clock clock = mock(Clock.class);
        final Locator locator = numericMap.keySet().iterator().next();
        when(clock.now()).thenReturn(new Instant(numericMap.get(locator).getCollectionTime() + MAX_AGE_ALLOWED - 1));

        // insertMetrics
        // write with datastax
        datastaxMetricsRW.insertMetrics( numericMap.values() );

        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        // just testing one metric
        assertNotNull(String.format("metric data for locator %s exists", locator), result);

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

        IMetric expectedMetric = numericMap.get(locator);
        assertNotNull( String.format( "point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals( String.format( "locator %s data is the same", locator ),
                ( new SimpleNumber( expectedMetric.getMetricValue() ) ),
                point.getData() );

        Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

        List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(numericMap.get(locator)); }};
        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
        assertEquals("Locators which are not delayed identified as delayed", 0, locatorsFromDBByShardAndSlot.size());
    }

    @Test
    public void testNumericSingleMetricDatapointsForRangeFullWithDelayedMetrics() throws IOException {

        Clock clock = mock(Clock.class);
        final Locator locator = numericMap.keySet().iterator().next();
        when(clock.now()).thenReturn(new Instant(numericMap.get(locator).getCollectionTime() + MAX_AGE_ALLOWED + 1));

        // insertMetrics
        // write with datastax
        MetricsRW datastaxMetricsRW1 = new DBasicMetricsRW(locatorIO, delayedLocatorIO, true, false, clock);
        datastaxMetricsRW1.insertMetrics( numericMap.values() );

        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        // just testing one metric
        assertNotNull(String.format("metric data for locator %s exists", locator), result);

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

        IMetric expectedMetric = numericMap.get(locator);
        assertNotNull( String.format( "point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals( String.format( "locator %s data is the same", locator ),
                ( new SimpleNumber( expectedMetric.getMetricValue() ) ),
                point.getData() );

        Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

        List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(numericMap.get(locator)); }};
        Set<Locator> ingestedDelayedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDBByShardAndSlot.containsAll(ingestedDelayedLocators));
    }

    /**
     * When writing Strings, astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testNumericSingleMetricDatapointsRange( Granularity granularity ) throws IOException {

        // write with datastax
        List<SingleRollupWriteContext> writeContexts = toWriteContext( numericMap.values(), granularity);
        datastaxMetricsRW.insertRollups( writeContexts );

        Locator locator = numericMap.keySet().iterator().next();

        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = numericMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        BasicRollup rollup = (BasicRollup)pointMap.get(expectedMetric.getCollectionTime()).getData();
        assertEquals( String.format( "locator %s average is the same", locator ),
                expectedMetric.getMetricValue(),
                rollup.getAverage().toLong() );
        assertEquals( String.format( "locator %s max value is the same", locator ),
                expectedMetric.getMetricValue(),
                rollup.getMaxValue().toLong() );
        assertEquals( String.format( "locator %s min value is the same", locator ),
                expectedMetric.getMetricValue(),
                rollup.getMinValue().toLong() );
        assertEquals( String.format( "locator %s sum is the same", locator ),
                (Long)expectedMetric.getMetricValue(),
                rollup.getSum(), EPSILON );

    }

    @Test
    public void testSingleNumericMetricDataToRollup() throws IOException {

        datastaxMetricsRW.insertMetrics( numericMap.values() );

        // pick first locator from input metrics, read with astyanaxMetricsRW.getDataToRollup
        final Locator locator = numericMap.keySet().iterator().next();

        IMetric expectedMetric = numericMap.get(locator);
        Points points =
                astyanaxMetricsRW.getDataToRollup(locator,
                        expectedMetric.getRollupType(),
                        getRangeFromMinAgoToNow(5),
                        CassandraModel.CF_METRICS_FULL_NAME);

        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals(String.format("locator %s data is the same", locator),
                (new SimpleNumber(expectedMetric.getMetricValue())), point.getData());

        Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

    }

    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testInsertNumericRollups( Granularity granularity ) throws IOException {

        // write with datastax
        List<SingleRollupWriteContext> writeContexts = toWriteContext( numericMap.values(), granularity);
        datastaxMetricsRW.insertRollups( writeContexts );

        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        // read with astyanax
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity);

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            BasicRollup rollup = (BasicRollup)pointMap.get(expectedMetric.getCollectionTime()).getData();
            assertEquals( String.format( "locator %s average is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getAverage().toLong() );
            assertEquals( String.format( "locator %s max value is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getMaxValue().toLong() );
            assertEquals( String.format( "locator %s min value is the same", locator ),
                    expectedMetric.getMetricValue(),
                    rollup.getMinValue().toLong() );
            assertEquals( String.format( "locator %s sum is the same", locator ),
                    (Long)expectedMetric.getMetricValue(),
                    rollup.getSum(), EPSILON );
        }
    }

    // no insertRollups for strings & booleans

}
