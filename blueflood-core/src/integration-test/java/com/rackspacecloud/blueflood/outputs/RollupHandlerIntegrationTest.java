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

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.handlers.RollupHandler;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class RollupHandlerIntegrationTest extends IntegrationTestBase {
    RollupHandler rollupHandler = new RollupHandler();

    private void writeFullData(
            Locator locator,
            long baseMillis,
            int hours,
            AstyanaxWriter writer) throws Exception {
        // insert something every minute for 48h
        for (int i = 0; i < 60 * hours; i++) {
            final long curMillis = baseMillis + i * 60000;
            List<Metric> metrics = new ArrayList<Metric>();
            metrics.add(getRandomIntMetric(locator, curMillis));
            writer.insertFull(metrics);
        }
    }

    @Test
    public void testRollupsOnReadGenerationLeft() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1432147283000L; // some point during 20 May 2015.
        int hours = 48;
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer," + randString(8);
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        final long offset = 15*60*1000; // offset for generating missing rollups
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        List<String> locatorList = new ArrayList<String>();
        locatorList.add(metricName);

        writeFullData(locator, baseMillis, hours, writer);

        // Generate 5m rollups with missing ranges on the left
        ArrayList<SingleRollupWriteContext> writes = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.getRangesToRollup(Granularity.FULL, baseMillis + offset, endMillis)) {
            // each range should produce one average
            Points<SimpleNumber> input = reader.getDataToRoll(SimpleNumber.class, locator, range, CassandraModel.CF_METRICS_FULL);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);


            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.FULL.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL.coarser()),
                    range.start));
        }
        writer.insertRollups(writes);

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, locatorList, baseMillis, endMillis, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }

    @Test
    public void testRollupsOnReadGenerationRight() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1432147283000L; // some point during 20 May 2015.
        int hours = 48;
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer2," + randString(8);
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        final long offset = 15*60*1000; // offset for generating missing rollups
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        List<String> locatorList = new ArrayList<String>();
        locatorList.add(metricName);

        writeFullData(locator, baseMillis, hours, writer);

        // Generate 5m rollups with missing ranges on the left
        ArrayList<SingleRollupWriteContext> writes = new ArrayList<SingleRollupWriteContext>();
        for (Range range : Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis -  offset)) {
            // each range should produce one average
            Points<SimpleNumber> input = reader.getDataToRoll(SimpleNumber.class, locator, range, CassandraModel.CF_METRICS_FULL);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);


            writes.add(new SingleRollupWriteContext(basicRollup,
                    locator,
                    Granularity.FULL.coarser(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL.coarser()),
                    range.start));
        }
        writer.insertRollups(writes);

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, locatorList, baseMillis, endMillis, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }

    @Test
    public void testRollupsOnReadGenerationEntireRange() throws Exception {
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
        final long baseMillis = 1432147283000L; // some point during 20 May 2015.
        int hours = 48;
        final String acctId = "ac" + IntegrationTestBase.randString(8);
        final String metricName = "fooService,barServer3," + randString(8);
        final long endMillis = baseMillis + (1000 * 60 * 60 * hours);
        final Locator locator = Locator.createLocatorFromPathComponents(acctId, metricName);
        List<String> locatorList = new ArrayList<String>();
        locatorList.add(metricName);

        writeFullData(locator, baseMillis, hours, writer);

        // Entire 5m rollups are missing

        Map<Locator, MetricData> metricDataMap = rollupHandler.getRollupByGranularity(acctId, locatorList, baseMillis, endMillis, Granularity.MIN_5);
        Set<Map.Entry<Long, Points.Point>> points = metricDataMap.get(locator).getData().getPoints().entrySet();
        Iterator<Range> repairedRanges = Range.getRangesToRollup(Granularity.FULL, baseMillis, endMillis).iterator();
        for (Map.Entry<Long, Points.Point> point : points) {
            Assert.assertEquals(repairedRanges.next().getStart(), (long)point.getKey());
        }
    }
}
