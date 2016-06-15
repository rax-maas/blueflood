/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.rackspacecloud.blueflood.outputs;

import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.astyanax.ABasicMetricsRW;
import com.rackspacecloud.blueflood.io.datastax.DBasicMetricsRW;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.RollupHandler;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * This class writes FULL data for a 48 hour period and then rolls up only a 5 hour portion from the middle of the 48 hours.
 *
 * Then the following conditions are tested, for both Single Plot and Multi Plot:
 * <li> Where the front part of a requested range needs to be rolled-up-on-read and the back half is already rolled up.
 * <li> Where the back part of a requested range needs to be rolled-up-on-read and the front half is already rolled up.
 * <li> Where the entire range needs to be rolled up.
 *
 * These tests verify that the correct timestamp keys are generated as well as that the total average of all FULL values
 * and the returned rolled up values are within 5%.
 *
 * NOTE:
 * <li> The MPLOT tests depend upon CoreConfig.TURN_OFF_RR_MPLOT == false, which is currently the default setting.
 *
 */
public class RollupHandlerIntegrationTest extends IntegrationTestBase {
    RollupHandler rollupHandler = new RollupHandler();

    private String acctId = "rollupIntegrationTest" + IntegrationTestBase.randString(8);
    private List<String> metricList = new ArrayList<String>( Arrays.asList( "rollupHandlerIntegrationTest1," + randString( 8 ),
            "rollupHandlerIntegrationTest2," + randString( 8 ),
            "rollupHandlerIntegrationTest3," + randString( 8 ) ));

    private List<Locator> locatorList = new ArrayList<Locator>();

    private long hours = 48;
    private long startMS = 1432147283000L; // some point during 20 May 2015.
    private long endMS = startMS + (1000 * 60 * 60 * hours); // 48 hours of data

    private long startRollupMS = startMS + (1000 * 60 * 60 * (hours/2 - 5));
    private long endRollupMS = startMS + (1000 * 60 * 60 * (hours/2));

    // max possible value for random generator of metric values
    private final int MAX_METRIC_VALUE = 100;
    // epsilon for assert of differences between mean of full points and mean of coarser granularity (rolled) points
    private final double MEAN_EPSILON = 0.0025 * MAX_METRIC_VALUE;

    @Before
    public void initData() throws Exception {

        for( String metric : metricList ) {

            locatorList.add( Locator.createLocatorFromPathComponents( acctId, metric ) );
        }

        AbstractMetricsRW basicMetricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        writeFullData( basicMetricsRW );
        writeRollups( basicMetricsRW );
    }

    private void writeRollups( AbstractMetricsRW metricsRW ) throws Exception {


        for( Locator locator : locatorList ) {
            ArrayList<SingleRollupWriteContext> writes = new ArrayList<SingleRollupWriteContext>();
            for ( Range range : Range.getRangesToRollup( Granularity.FULL, startRollupMS, endRollupMS ) ) {
                // each range should produce one average
                Points<SimpleNumber> input = metricsRW.getDataToRollup(
                                                    locator,
                                                    RollupType.BF_BASIC,
                                                    range, CassandraModel.CF_METRICS_FULL_NAME );
                BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples( input );

                writes.add( new SingleRollupWriteContext( basicRollup,
                        locator,
                        Granularity.FULL.coarser(),
                        CassandraModel.getColumnFamily( BasicRollup.class, Granularity.FULL.coarser() ),
                        range.start ) );
            }
            metricsRW.insertRollups( writes );
        }
    }

    private void writeFullData( AbstractMetricsRW writer ) throws Exception {

        // insert something every minute for 48h
        for ( Locator locator : locatorList ) {
            for ( int i = 0; i < 60 * hours; i++ ) {
                final long curMillis = startMS + i * 60000;
                List<IMetric> metrics = new ArrayList<IMetric>();
                metrics.add( getRandomIntMetricMaxValue( locator, curMillis, MAX_METRIC_VALUE ) );
                writer.insertMetrics(metrics);
            }
        }
    }

    @Test
    public void testSplotRollupsOnReadGenerationLeft() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, startMS, endRollupMS, Granularity.MIN_5);
        Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get(locator).getData().getPoints();

        // test keys
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, startMS, endRollupMS).iterator();
        for (Long timestamp : points.keySet() ) {
            Assert.assertEquals( repairedRanges.next().getStart(), timestamp.longValue() );
        }

        // test value
        double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(startMS), endRollupMS );
        double rollMean = meanOfPointCollectionRoll( points.values() );

        Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
    }

    @Test
    public void testSplotRollupsOnReadGenerationRight() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, startRollupMS, endMS, Granularity.MIN_5);
        Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get(locator).getData().getPoints();

        // test keys
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, startRollupMS, endMS).iterator();
        for (Long timestamp : points.keySet() ) {
            Assert.assertEquals( repairedRanges.next().getStart(), timestamp.longValue() );
        }

        // test value
        double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(startRollupMS), endMS );
        double rollMean = meanOfPointCollectionRoll( points.values() );

        Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
    }

    @Test
    public void testSplotRollupsOnReadGenerationEntireRange() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        // start 1 hour after rollups ended
        long start = endRollupMS + 1000 * 60 * 60;

        // test keys
        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, start, endMS, Granularity.MIN_5);
        Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get(locator).getData().getPoints();

        Assert.assertNotEquals("there are more than one points fetched", 0, points.size());

        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, start, endMS).iterator();
        for (Long timestamp : points.keySet()) {
            Assert.assertEquals(repairedRanges.next().getStart(), timestamp.longValue() );
        }

        // test value
        double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(start), endMS );
        double rollMean = meanOfPointCollectionRoll( points.values() );

        Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
    }

    @Test
    public void testMplotRollupsOnReadGenerationLeft() throws Exception {

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metricList, startMS, endRollupMS, Granularity.MIN_5);

        for( int i = 0; i < locatorList.size(); i++ ) {

            Locator locator = locatorList.get( i );
            List<String> metric = new ArrayList<String>();
            metric.add( metricList.get( i ) );

            // test keys
            Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get( locator ).getData().getPoints();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, startMS, endRollupMS ).iterator();
            for ( Long timestamp : points.keySet() ) {
                Assert.assertEquals( repairedRanges.next().getStart(), timestamp.longValue() );
            }

            // test value
            double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(startMS), endRollupMS );
            double rollMean = meanOfPointCollectionRoll( points.values() );

            Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
        }
    }

    @Test
    public void testMplotRollupsOnReadGenerationRight() throws Exception {

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metricList, startRollupMS, endMS, Granularity.MIN_5);

        for( int i = 0; i < locatorList.size(); i++ ) {

            Locator locator = locatorList.get( i );
            List<String> metric = new ArrayList<String>();
            metric.add( metricList.get( i ) );

            // test keys
            Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get( locator ).getData().getPoints();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, startRollupMS, endMS ).iterator();
            for ( Long timestamp : points.keySet() ) {
                Assert.assertEquals( repairedRanges.next().getStart(), timestamp.longValue() );
            }

            // test value
            double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(startRollupMS), endMS );
            double rollMean = meanOfPointCollectionRoll( points.values() );

            Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
        }
    }

    @Test
    public void testMplotRollupsOnReadGenerationEntireRange() throws Exception {

        // start 1 hour after rollups ended
        long start = endRollupMS + 1000 * 60 * 60;

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity( acctId, metricList, start, endMS, Granularity.MIN_5 );

        for( int i = 0; i < locatorList.size(); i++ ) {

            Locator locator = locatorList.get( i );
            List<String> metric = new ArrayList<String>();
            metric.add( metricList.get( i ) );

            // test keys
            Map<Long, Points.Point<BasicRollup>> points = metricDataMap.get( locator ).getData().getPoints();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, start, endMS ).iterator();
            for ( Long timestamp : points.keySet() ) {
                Assert.assertEquals( repairedRanges.next().getStart(), timestamp.longValue() );
            }

            // test value
            double fullMean = fullPointsMean( metric, locator, Granularity.MIN_5.snapMillis(start), endMS );
            double rollMean = meanOfPointCollectionRoll( points.values() );

            Assert.assertEquals( fullMean, rollMean, MEAN_EPSILON );
        }
    }

    private double meanOfPointCollectionFull( Collection<Points.Point<SimpleNumber>> fullPoints ) {
        double sum = 0;
        for( Points.Point<SimpleNumber> p : fullPoints ) {

            sum += p.getData().getValue().intValue();
        }

        return sum / fullPoints.size();
    }

    private double meanOfPointCollectionRoll( Collection<Points.Point<BasicRollup>> fullPoints ) {
        double sum = 0;
        for( Points.Point<BasicRollup> p : fullPoints ) {

            sum += p.getData().getAverage().toLong();
        }

        return sum / (double) fullPoints.size();
    }

    private double fullPointsMean( List<String> metric, Locator locator, long start, long end ) {

        Collection<Points.Point<SimpleNumber>> fullPoints = rollupHandler.getRollupByGranularity( acctId, metric, start, end, Granularity.FULL )
                .get( locator ).getData().getPoints().values();
        return meanOfPointCollectionFull( fullPoints );
    }

}
