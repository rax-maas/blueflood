package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.io.astyanax.ABasicMetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verify MetricsRW for basic metrics (SimpleNumber, String, Boolean) implementations, here when Astyanax is writing and
 * Datastax is reading.
 */
@RunWith( JUnitParamsRunner.class )
public class BasicMetricsRWAsytanaxWriteIntegrationTest extends BasicMetricsRWIntegrationTest {

    @Test
    public void testNumericMultiMetricsDatapointsRangeFull() throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( numericMap.values() );

        // read with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
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
        assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
    }

    @Test
    public void testNumericMultiMetricsDatapointsRangeFullWithDelayedMetrics() throws IOException {

        MetricsRW astyanaxMetricsRW1 = new ABasicMetricsRW(true, new DefaultClockImpl());

        //making one metric delayed
        final Locator delayedLocator = numericMap.keySet().iterator().next();
        IMetric currentMetric = numericMap.get(delayedLocator);
        final IMetric delayedMetric = new Metric(currentMetric.getLocator(), currentMetric.getMetricValue(),
                currentMetric.getCollectionTime() - MAX_AGE_ALLOWED - 1000, new TimeValue(currentMetric.getTtlInSeconds(),
                TimeUnit.SECONDS), "unit");
        numericMap.put(delayedLocator, delayedMetric);

        // write with astyanax
        astyanaxMetricsRW1.insertMetrics( numericMap.values() );

        // read with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5 + (int ) (MAX_AGE_ALLOWED / 60 / 1000)),
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

        MetricsRW astyanaxMetricsRW1 = new ABasicMetricsRW(true, clock);

        // write with astyanax
        astyanaxMetricsRW1.insertMetrics( numericMap.values() );

        // read with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL);

        assertEquals("number of locators", numericMap.keySet().size(), results.keySet().size());

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
    @Parameters(method = "getGranularitiesToTest" )
    public void testNumericMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with astyanax
        List<SingleRollupWriteContext> writeContexts = toWriteContext(numericMap.values(), granularity);
        astyanaxMetricsRW.insertRollups(writeContexts);

        // write with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity);

        assertEquals("number of locators", numericMap.keySet().size(), results.keySet().size());

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size());

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
            assertEquals(String.format("locator %s min value is the same", locator),
                    expectedMetric.getMetricValue(),
                    rollup.getMinValue().toLong());
            assertEquals( String.format( "locator %s sum is the same", locator ),
                    (Long)expectedMetric.getMetricValue(),
                    rollup.getSum(), EPSILON );
        }
    }


    @Test
    public void testStringMultiMetricsDatapointsRangeFull() throws IOException, InterruptedException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( stringMap.values() );

        // write with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(stringMap.keySet());
        }};

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL);

        assertEquals("number of locators", stringMap.keySet().size(), results.keySet().size());

        for ( Map.Entry<Locator, IMetric> entry : stringMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull(String.format("point for locator %s at timestamp %s exists",
                    locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime())));

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
            assertEquals( String.format( "locator %s data is the same", locator ),
                    expectedMetric.getMetricValue(),
                    point.getData() );

        }

        Set<Locator> locatorsFromDB = retrieveLocators(stringMap.keySet());
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(stringMap.values()));
        assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
    }

    /**
     * When writing Strings, astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testStringMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( stringMap.values() );

        // write with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(stringMap.keySet());
        }};

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertEquals( "number of locators", stringMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : stringMap.entrySet() ) {
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

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
            assertEquals(String.format("locator %s data is the same", locator),
                    expectedMetric.getMetricValue(),
                    point.getData());
        }

        Set<Locator> locatorsFromDB = retrieveLocators(stringMap.keySet());
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(stringMap.values()));
        assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
    }


    @Test
    public void testBooleanMultiMetricsDatapointsRangeFull() throws IOException, InterruptedException {

        // write with astyanax
        //datastaxMetricsRW.insertMetrics( boolMap.values() );
        astyanaxMetricsRW.insertMetrics( boolMap.values() );

        // write with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(boolMap.keySet());
        }};

        for ( Map.Entry<Locator, IMetric> entry : boolMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = datastaxMetricsRW.getDatapointsForRange(locator,
                    getRangeFromMinAgoToNow(5),
                    Granularity.FULL);
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator,
                    expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            assertEquals( String.format( "locator %s data is the same", locator ),
                    expectedMetric.getMetricValue(),
                    point.getData() );
        }

        Set<Locator> locatorsFromDB = retrieveLocators(boolMap.keySet());
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(boolMap.values()));
        assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
    }

    /**
     * When writing Booleans (Strings), astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testBooleanMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( boolMap.values() );

        // write with datastaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(boolMap.keySet());
        }};

        for ( Map.Entry<Locator, IMetric> entry : boolMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = datastaxMetricsRW.getDatapointsForRange( locator,
                    getRangeFromMinAgoToNow(5),
                    granularity );
            assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                    locator,
                    expectedMetric.getCollectionTime(),
                    pointMap.get( expectedMetric.getCollectionTime() ) ) );

            Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
            assertEquals( String.format( "locator %s data is the same", locator ),
                    expectedMetric.getMetricValue(),
                    point.getData() );
        }

        Set<Locator> locatorsFromDB = retrieveLocators(boolMap.keySet());
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());

        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(new ArrayList<IMetric>(boolMap.values()));
        assertTrue("Locators which are not delayed identified as delayed", Collections.disjoint(locatorsFromDBByShardAndSlot, locators));
    }

    @Test
    public void testNumericSingleMetricDatapointsForRangeFull() throws IOException {

        Clock clock = mock(Clock.class);
        final Locator locator = numericMap.keySet().iterator().next();
        when(clock.now()).thenReturn(new Instant(numericMap.get(locator).getCollectionTime() + MAX_AGE_ALLOWED - 1));

        // insertMetrics
        // write with astyanax
        astyanaxMetricsRW.insertMetrics(numericMap.values());

        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL);

        // just testing one metric
        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = numericMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals(String.format("locator %s data is the same", locator),
                (new SimpleNumber(expectedMetric.getMetricValue())),
                point.getData());

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
        // write with astyanax
        MetricsRW astyanaxMetricsRW1 = new ABasicMetricsRW(true, clock);
        astyanaxMetricsRW1.insertMetrics( numericMap.values() );

        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL);

        // just testing one metric
        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size() );

        IMetric expectedMetric = numericMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals(String.format("locator %s data is the same", locator),
                (new SimpleNumber(expectedMetric.getMetricValue())),
                point.getData());

        Set<Locator> ingestedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDB = retrieveLocators(ingestedLocators);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDB.containsAll(ingestedLocators));

        List<IMetric> ingestedDelayedMetrics = new ArrayList<IMetric>(){{ add(numericMap.get(locator)); }};
        Set<Locator> ingestedDelayedLocators = new HashSet<Locator>(){{ add(locator); }};
        Set<Locator> locatorsFromDBByShardAndSlot = retrieveLocatorsByShardAndSlot(ingestedDelayedMetrics);
        assertTrue("Some of the ingested locator's missing from db", locatorsFromDBByShardAndSlot.containsAll(ingestedDelayedLocators));

    }

    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testNumericSingleMetricDatapointsRange( Granularity granularity ) throws IOException {

        // write with astyanax
        List<SingleRollupWriteContext> writeContexts = toWriteContext(numericMap.values(), granularity);
        astyanaxMetricsRW.insertRollups(writeContexts );

        Locator locator = numericMap.keySet().iterator().next();

        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull(String.format("metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size() );

        IMetric expectedMetric = numericMap.get( locator );
        assertNotNull(String.format("point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime() ) ) );

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
    public void testStringSingleMetricDatapointsForRangeFull() throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( stringMap.values() );
        final Locator locator = stringMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertNotNull(String.format("metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = stringMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals(String.format("locator %s data is the same", locator),
                expectedMetric.getMetricValue(),
                point.getData());

        Set<Locator> locatorsFromDB = retrieveLocators(new HashSet<Locator>() {{
            add(locator); }});
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());
    }

    /**
     * When writing Strings, astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testStringSingleMetricDatapointsForRange( Granularity granularity ) throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics(stringMap.values() );
        final Locator locator = stringMap.keySet().iterator().next();

        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull(String.format("metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = stringMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals(String.format("locator %s data is the same", locator),
                expectedMetric.getMetricValue(),
                point.getData());

        Set<Locator> locatorsFromDB = retrieveLocators(new HashSet<Locator>() {{
            add(locator);
        }});
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());
    }

    @Test
    public void testBooleanSingleMetricDatapointsForRangeFull() throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics(boolMap.values() );
        final Locator locator = boolMap.keySet().iterator().next();

        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertNotNull(String.format("metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = boolMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals(String.format("locator %s data is the same", locator),
                expectedMetric.getMetricValue(),
                point.getData());

        Set<Locator> locatorsFromDB = retrieveLocators(new HashSet<Locator>() {{
            add(locator);
        }});
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());
    }

    /**
     * When writing Booleans (Strings), astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testBooleanSingleMetricDatapointsForRange( Granularity granularity ) throws IOException {

        // write with astyanax
        astyanaxMetricsRW.insertMetrics( boolMap.values() );
        final Locator locator = boolMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        MetricData result = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull(String.format("metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = boolMap.get(locator);
        assertNotNull(String.format("point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get(expectedMetric.getCollectionTime())));

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals(String.format("locator %s data is the same", locator),
                expectedMetric.getMetricValue(),
                point.getData());

        Set<Locator> locatorsFromDB = retrieveLocators(new HashSet<Locator>() {{
            add(locator);
        }});
        assertEquals("String/Boolean locator's inserted in metrics_locator CF", 0, locatorsFromDB.size());
    }

    @Test
    public void testSingleNumericMetricDataToRollup() throws IOException {

        astyanaxMetricsRW.insertMetrics( numericMap.values() );

        // pick first locator from input metrics, write with datastaxMetricsRW.getDataToRollup
        final Locator locator = numericMap.keySet().iterator().next();

        IMetric expectedMetric = numericMap.get(locator);
        Points points =
                datastaxMetricsRW.getDataToRollup(locator,
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

    /**
     * getDataToRollup() should never be called on the String table.
     */
    @Test( expected = IOException.class )
    public void testSingleStringMetricDataToRollup() throws IOException {

        astyanaxMetricsRW.insertMetrics( stringMap.values() );

        // pick first locator from input metrics, write with datastaxMetricsRW.getDataToRollup
        Locator locator = stringMap.keySet().iterator().next();

        IMetric expectedMetric = stringMap.get(locator);
        datastaxMetricsRW.getDataToRollup(locator,
                expectedMetric.getRollupType(),
                getRangeFromMinAgoToNow(5),
                CassandraModel.CF_METRICS_STRING_NAME );
    }

    /**
     * getDataToRollup() should never be called on the String table.
     */
    @Test( expected = IOException.class )
    public void testSingleBooleanMetricDataToRollup() throws IOException {

        astyanaxMetricsRW.insertMetrics( boolMap.values() );

        // pick first locator from input metrics, write with datastaxMetricsRW.getDataToRollup
        Locator locator = boolMap.keySet().iterator().next();

        IMetric expectedMetric = boolMap.get(locator);
        datastaxMetricsRW.getDataToRollup(locator,
                expectedMetric.getRollupType(),
                getRangeFromMinAgoToNow(5),
                CassandraModel.CF_METRICS_STRING_NAME );
    }

    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testInsertNumericRollups( Granularity granularity ) throws IOException {

        // write with astyanax
        List<SingleRollupWriteContext> writeContexts = toWriteContext( numericMap.values(), granularity);
        astyanaxMetricsRW.insertRollups( writeContexts );

        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        // write with datastax
        Map<Locator, MetricData> results = datastaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity);

        assertEquals( "number of locators", numericMap.keySet().size(), results.keySet().size() );

        for ( Map.Entry<Locator, IMetric> entry : numericMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = results.get(locator);
            assertNotNull(String.format("metric data for locator %s exists", locator), metricData );

            Points points = metricData.getData();
            Map<Long, Points.Point> pointMap = points.getPoints();
            assertEquals(String.format("number of points for locator %s", locator), 1, pointMap.values().size() );

            IMetric expectedMetric = entry.getValue();
            assertNotNull(String.format("point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                    pointMap.get(expectedMetric.getCollectionTime() ) ) );

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

    // This is maintaining broken functionality from astyanax.  You can't put a rollup into metrics_full,
    // but astyanax allows you to.  However, when you attempt to read it, you get 0 points.
    @Test
    public void testInsertNumericRollupFull() throws IOException {

        Map.Entry<Locator, IMetric> entry = numericMap.entrySet().iterator().next();
        Locator locator = entry.getKey();


        List<SingleRollupWriteContext> cxts = new ArrayList<SingleRollupWriteContext>();

        cxts.add( createSingleRollupWriteContext( Granularity.FULL, entry.getValue() ) );

        astyanaxMetricsRW.insertRollups( cxts );


        // write with datastax
        MetricData metricData = datastaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), metricData );

        Points points = metricData.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "no points for locator %s", locator ), 0, pointMap.values().size() );
    }

    // no insertRollups for strings & booleans
}
