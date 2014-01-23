/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.tools;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.tools.ops.RollupTool;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RollupToolTest {

    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    private AstyanaxReader reader = AstyanaxReader.getInstance();
    private final Locator testLocator = Locator.createLocatorFromPathComponents("tenantId", "metricName");
    private final Range range = new Range(0, Granularity.MIN_1440.milliseconds() - 1);

    @Before
    public void setUp() throws Exception{
        final TimeValue ttl = new TimeValue(48, TimeUnit.HOURS);

        // write some full resolution data.
        Collection<IMetric> normalMetrics = new ArrayList<IMetric>();
        long time = Granularity.MIN_5.milliseconds()/2;

        //Values 1-288 located at the mid-point of every 5m slot. So 288 5m rollups, 288/4 20 m rollups and so on
        for (int i = 1; i < 289; i++) {
            //every raw metric slot centered at the mid point of 5m slot
            IMetric metric;
            metric = new Metric(testLocator, i, time, ttl, "call_me_unit");
            normalMetrics.add(metric);
            time = time + Granularity.MIN_5.milliseconds();
        }
        writer.insertMetrics(normalMetrics, CassandraModel.CF_METRICS_FULL);
    }

    @Test
    public void testRollupTool() throws Exception{
        RollupTool.rerollData(testLocator, range);
        test5mRollups();
        test20mRollups();
        test60mRollups();
        test240mRollups();
        test1440mRollups();
    }

    private void test5mRollups() throws IOException {
        Points<BasicRollup> points =  reader.getDataToRoll(BasicRollup.class,
                testLocator,
                range,
                CassandraModel.CF_METRICS_5M);
        Assert.assertEquals(288, points.getPoints().size());
        int i=1;
        for (Map.Entry<Long, Points.Point<BasicRollup>> pointsEntry : points.getPoints().entrySet()) {
            Assert.assertEquals(pointsEntry.getValue().getData().getMaxValue().toLong(),i);
            Assert.assertEquals(pointsEntry.getValue().getData().getMinValue().toLong(), i);
            i++;
        }
    }

    private void test20mRollups() throws IOException {
        Points<BasicRollup> points =  reader.getDataToRoll(BasicRollup.class,
                testLocator,
                range,
                CassandraModel.CF_METRICS_20M);
        Assert.assertEquals(72, points.getPoints().size());
        int i=1;
        for (Map.Entry<Long, Points.Point<BasicRollup>> pointsEntry : points.getPoints().entrySet()) {
            Assert.assertEquals(pointsEntry.getValue().getData().getMinValue().toLong(), i);
            Assert.assertEquals(pointsEntry.getValue().getData().getMaxValue().toLong(),i+3);
            i=i+4;
        }
    }

    private void test60mRollups() throws IOException {
        Points<BasicRollup> points =  reader.getDataToRoll(BasicRollup.class,
                testLocator,
                range,
                CassandraModel.CF_METRICS_60M);
        Assert.assertEquals(24, points.getPoints().size());
        int i=1;
        for (Map.Entry<Long, Points.Point<BasicRollup>> pointsEntry : points.getPoints().entrySet()) {
            Assert.assertEquals(pointsEntry.getValue().getData().getMinValue().toLong(), i);
            Assert.assertEquals(pointsEntry.getValue().getData().getMaxValue().toLong(),i+11);
            i=i+12;
        }
    }

    private void test240mRollups() throws IOException {
        Points<BasicRollup> points =  reader.getDataToRoll(BasicRollup.class,
                testLocator,
                range,
                CassandraModel.CF_METRICS_240M);
        Assert.assertEquals(6, points.getPoints().size());
        int i=1;
        for (Map.Entry<Long, Points.Point<BasicRollup>> pointsEntry : points.getPoints().entrySet()) {
            Assert.assertEquals(pointsEntry.getValue().getData().getMinValue().toLong(), i);
            Assert.assertEquals(pointsEntry.getValue().getData().getMaxValue().toLong(),i+47);
            i=i+48;
        }
    }

    private void test1440mRollups() throws IOException {
        Points<BasicRollup> points =  reader.getDataToRoll(BasicRollup.class,
                testLocator,
                range,
                CassandraModel.CF_METRICS_1440M);
        Assert.assertEquals(1, points.getPoints().size());
        int i=1;
        for (Map.Entry<Long, Points.Point<BasicRollup>> pointsEntry : points.getPoints().entrySet()) {
            Assert.assertEquals(pointsEntry.getValue().getData().getMinValue().toLong(), i);
            Assert.assertEquals(pointsEntry.getValue().getData().getMaxValue().toLong(),i+287);
            i=i+288;
        }
    }
}
