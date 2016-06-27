package com.rackspacecloud.blueflood.io;

import java.util.Map;

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

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
    @Parameters( method ="getGranularitiesToTest" )
    public void testNumericMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with datastax
        List<SingleRollupWriteContext> writeContexts = toWriteContext( numericMap.values(), granularity);
        datastaxMetricsRW.insertRollups( writeContexts );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(numericMap.keySet());
        }};
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                granularity );

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
    public void testStringMultiMetricsDatapointsRangeFull() throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( stringMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(stringMap.keySet());
        }};

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
                locators,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

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
            assertEquals( String.format( "locator %s data is the same", locator ),
                    expectedMetric.getMetricValue(),
                    point.getData() );
        }
    }

    /**
     * When writing Strings, astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testStringMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( stringMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(stringMap.keySet());
        }};

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        Map<Locator, MetricData> results = astyanaxMetricsRW.getDatapointsForRange(
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
            assertEquals( String.format( "locator %s data is the same", locator ),
                    expectedMetric.getMetricValue(),
                    point.getData() );
        }
    }

    @Test
    public void testBooleanMultiMetricsDatapointsRangeFull() throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( boolMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(boolMap.keySet());
        }};

        for ( Map.Entry<Locator, IMetric> entry : boolMap.entrySet() ) {
            Locator locator = entry.getKey();

            MetricData metricData = astyanaxMetricsRW.getDatapointsForRange( locator,
                    getRangeFromMinAgoToNow(5),
                    Granularity.FULL );
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
    }

    /**
     * When writing Booleans (Strings), astyanax ignores granularity and always writes to string table
     */
    @Test
    @Parameters( method ="getGranularitiesToTest" )
    public void testBooleanMultiMetricsDatapointsRange( Granularity granularity ) throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( boolMap.values() );

        // read with astyanaxRW.getDatapointsForRange()
        List<Locator> locators = new ArrayList<Locator>() {{
            addAll(boolMap.keySet());
        }};

        for ( Map.Entry<Locator, IMetric> entry : boolMap.entrySet() ) {
            Locator locator = entry.getKey();

            // Note, i'm not using AstyanaxReader.getDatapointsForRange( List<Locators>, ... ) because it assumes everything
            // is numeric with AstyanaxReader.getNumericDataForRangeLocatorList()
            MetricData metricData = astyanaxMetricsRW.getDatapointsForRange( locator,
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
    }

    @Test
    public void testNumericSingleMetricDatapointsForRangeFull() throws IOException {

        // insertMetrics
        // write with datastax
        datastaxMetricsRW.insertMetrics( numericMap.values() );

        Locator locator = numericMap.keySet().iterator().next();

        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        // just testing one metric
        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = numericMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals( String.format( "locator %s data is the same", locator ),
                ( new SimpleNumber( expectedMetric.getMetricValue() ) ),
                point.getData() );
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
    public void testStringSingleMetricDatapointsForRangeFull() throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( stringMap.values() );
        Locator locator = stringMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = stringMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals( String.format( "locator %s data is the same", locator ),
                expectedMetric.getMetricValue(),
                point.getData() );
    }

    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testStringSingleMetricDatapointsForRange( Granularity granularity ) throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( stringMap.values() );
        Locator locator = stringMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        // (doesn't support booleans)
        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = stringMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals( String.format( "locator %s data is the same", locator ),
                expectedMetric.getMetricValue(),
                point.getData() );
    }


    @Test
    public void testBooleanSingleMetricDatapointsForRangeFull() throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( boolMap.values() );
        Locator locator = boolMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                Granularity.FULL );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = boolMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals( String.format( "locator %s data is the same", locator ),
                expectedMetric.getMetricValue(),
                point.getData() );
    }

    @Test
    @Parameters( method = "getGranularitiesToTest" )
    public void testBooleanSingleMetricDatapointsForRange( Granularity granularity ) throws IOException {

        // write with datastax
        datastaxMetricsRW.insertMetrics( boolMap.values() );
        Locator locator = boolMap.keySet().iterator().next();

        // this call supports strings, although I'm not sure sure they meant to.
        MetricData result = astyanaxMetricsRW.getDatapointsForRange(
                locator,
                getRangeFromMinAgoToNow(5),
                granularity );

        assertNotNull( String.format( "metric data for locator %s exists", locator ), result );

        Points points = result.getData();
        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        IMetric expectedMetric = boolMap.get( locator );
        assertNotNull( String.format( "point for locator %s at timestamp %s exists",
                locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime() );
        assertEquals( String.format( "locator %s data is the same", locator ),
                expectedMetric.getMetricValue(),
                point.getData() );
    }

    @Test
    public void testSingleNumericMetricDataToRollup() throws IOException {

        datastaxMetricsRW.insertMetrics( numericMap.values() );

        // pick first locator from input metrics, read with astyanaxMetricsRW.getDataToRollup
        Locator locator = numericMap.keySet().iterator().next();

        IMetric expectedMetric = numericMap.get(locator);
        Points points =
                astyanaxMetricsRW.getDataToRollup(locator,
                        expectedMetric.getRollupType(),
                        getRangeFromMinAgoToNow(5),
                        CassandraModel.CF_METRICS_FULL_NAME);

        Map<Long, Points.Point> pointMap = points.getPoints();
        assertEquals( String.format( "number of points for locator %s", locator ), 1, pointMap.values().size() );

        assertNotNull( String.format( "point for locator %s at timestamp %s exists", locator, expectedMetric.getCollectionTime(),
                pointMap.get( expectedMetric.getCollectionTime() ) ) );

        Points.Point point = pointMap.get(expectedMetric.getCollectionTime());
        assertEquals( String.format( "locator %s data is the same", locator ),
                ( new SimpleNumber( expectedMetric.getMetricValue() ) ), point.getData() );

    }

    @Test( expected = IOException.class )
    public void testSingleStringMetricDataToRollup() throws IOException {

        datastaxMetricsRW.insertMetrics( stringMap.values() );

        // pick first locator from input metrics, read with astyanaxMetricsRW.getDataToRollup
        Locator locator = stringMap.keySet().iterator().next();

        IMetric expectedMetric = stringMap.get(locator);
        astyanaxMetricsRW.getDataToRollup(locator,
                expectedMetric.getRollupType(),
                getRangeFromMinAgoToNow(5),
                CassandraModel.CF_METRICS_STRING_NAME );
    }

    @Test( expected = IOException.class )
    public void testSingleBooleanMetricDataToRollup() throws IOException {

        datastaxMetricsRW.insertMetrics( boolMap.values() );

        // pick first locator from input metrics, read with astyanaxMetricsRW.getDataToRollup
        Locator locator = boolMap.keySet().iterator().next();

        IMetric expectedMetric = boolMap.get(locator);
        astyanaxMetricsRW.getDataToRollup(locator,
                expectedMetric.getRollupType(),
                getRangeFromMinAgoToNow(5),
                CassandraModel.CF_METRICS_STRING_NAME );
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
