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

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.RollupHandler;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * NOTE:
 * <li> The MPLOT tests depend upon CoreConfig.TURN_OFF_RR_MPLOT == false, which is currently the default setting.
 * <li> These tests only are verifying the key generation of the Rollup process, not the generated data values.
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

    @Before
    public void initData() throws Exception {

        for( String metric : metricList ) {

            locatorList.add( Locator.createLocatorFromPathComponents( acctId, metric ) );
        }

        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();

        writeFullData( writer );
        writeRollups( reader, writer );
    }

    private void writeRollups( AstyanaxReader reader, AstyanaxWriter writer ) throws GranularityException, java.io.IOException, ConnectionException {


        for( Locator locator : locatorList ) {
            ArrayList<SingleRollupWriteContext> writes = new ArrayList<SingleRollupWriteContext>();
            for ( Range range : Range.getRangesToRollup( Granularity.FULL, startRollupMS, endRollupMS ) ) {
                // each range should produce one average
                Points<SimpleNumber> input = reader.getDataToRoll( SimpleNumber.class, locator, range, CassandraModel.CF_METRICS_FULL );
                BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples( input );

                writes.add( new SingleRollupWriteContext( basicRollup,
                        locator,
                        Granularity.FULL.coarser(),
                        CassandraModel.getColumnFamily( BasicRollup.class, Granularity.FULL.coarser() ),
                        range.start ) );
            }
            writer.insertRollups( writes );
        }
    }

    private void writeFullData( AstyanaxWriter writer ) throws Exception {

        // insert something every minute for 48h
        for ( Locator locator : locatorList ) {
            for ( int i = 0; i < 60 * hours; i++ ) {
                final long curMillis = startMS + i * 60000;
                List<Metric> metrics = new ArrayList<Metric>();
                metrics.add( getRandomIntMetric( locator, curMillis ) );
                writer.insertFull( metrics );
            }
        }
    }

    @Test
    public void testSplotRollupsOnReadGenerationLeft() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, startMS, endRollupMS, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, startMS, endRollupMS).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }

    @Test
    public void testSplotRollupsOnReadGenerationRight() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, startRollupMS, endMS, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, startRollupMS, endMS).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }

    @Test
    public void testSplotRollupsOnReadGenerationEntireRange() throws Exception {

        List<String> metric = new ArrayList<String>();
        metric.add( metricList.get( 0 ) );
        Locator locator = locatorList.get( 0 );

        // start 1 hour after rollups ended
        long start = endRollupMS + 1000 * 60 * 60;

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metric, start, endMS, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, start, endMS).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }

    @Test
    public void testMplotRollupsOnReadGenerationLeft() throws Exception {

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metricList, startMS, endRollupMS, Granularity.MIN_5);

        for( Locator locator : locatorList ) {

            Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get( locator ).getData().getPoints().entrySet();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, startMS, endRollupMS ).iterator();
            for ( Map.Entry<Long, Points.Point> point : points ) {
                Assert.assertEquals( repairedRanges.next().getStart(), (long) point.getKey() );
            }
        }
    }

    @Test
    public void testMplotRollupsOnReadGenerationRight() throws Exception {

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, metricList, startRollupMS, endMS, Granularity.MIN_5);

        for( Locator locator : locatorList ) {

            Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get( locator ).getData().getPoints().entrySet();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, startRollupMS, endMS ).iterator();
            for ( Map.Entry<Long, Points.Point> point : points ) {
                Assert.assertEquals( repairedRanges.next().getStart(), (long) point.getKey() );
            }
        }
    }

    @Test
    public void testMplotRollupsOnReadGenerationEntireRange() throws Exception {

        // start 1 hour after rollups ended
        long start = endRollupMS + 1000 * 60 * 60;

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity( acctId, metricList, start, endMS, Granularity.MIN_5 );

        for ( Locator locator : locatorList ) {

            Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get( locator ).getData().getPoints().entrySet();
            Iterator<Range> repairedRanges = Range.getRangesToRollup( Granularity.FULL, start, endMS ).iterator();
            for ( Map.Entry<Long, Points.Point> point : points ) {
                Assert.assertEquals( repairedRanges.next().getStart(), (long) point.getKey() );
            }
        }
    }
}
