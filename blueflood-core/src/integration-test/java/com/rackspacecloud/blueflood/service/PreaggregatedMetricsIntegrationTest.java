/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.service;

import com.google.common.collect.Lists;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// todo: need an easy way to run this. It will require some plumbing changes to the project.
public class PreaggregatedMetricsIntegrationTest extends IntegrationTestBase {
    
    private BluefloodTimerRollup simple;
    private static final TimeValue ttl = new TimeValue(24, TimeUnit.HOURS);
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    private AstyanaxReader reader = AstyanaxReader.getInstance();
    private static final AtomicLong timestamp = new AtomicLong(10);
    
    @Before
    public void createFixtures() throws Exception {
        simple = new BluefloodTimerRollup()
            .withSampleCount(1)
            .withSum(100d)
            .withCountPS(101d)
            .withAverage(102L)
            .withVariance(103d)
            .withMinValue(104)
            .withMaxValue(105)
            .withCount(106);
        simple.setPercentile("98th", 107);
        simple.setPercentile("99th", 110);
    }
    
    private static Points<BluefloodTimerRollup> getTimerDataToRoll(AstyanaxReader reader, Locator locator, Range range, Granularity gran) throws IOException {
        ColumnFamily<Locator, Long> cf = CassandraModel.getColumnFamily(BluefloodTimerRollup.class, gran);
        return reader.getDataToRoll(BluefloodTimerRollup.class, locator, range, cf);
    }
    
    @Test
    public void testFullReadWrite() throws Exception {
        long ts = timestamp.incrementAndGet();
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "full", "read", "put");
        IMetric metric = new PreaggregatedMetric(ts, locator, ttl, simple);

        writer.insertMetrics(Lists.newArrayList(metric), CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        
        Points<BluefloodTimerRollup> points = PreaggregatedMetricsIntegrationTest.getTimerDataToRoll(reader, locator, new Range(ts, ts+1), Granularity.FULL);

        Assert.assertEquals(1, points.getPoints().size());
        Assert.assertEquals(metric.getMetricValue(), points.getPoints().get(ts).getData());
    }
    
    @Test
    public void testHigherGranReadWrite() throws Exception {
        final long ts = timestamp.incrementAndGet();
        final long rollupTs = ts + 100;
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "rollup", "read", "put");
        IMetric metric = new PreaggregatedMetric(ts, locator, ttl, simple);
        
        writer.insertMetrics(Lists.newArrayList(metric), CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        
        // read the raw data.
        Points<BluefloodTimerRollup> points = PreaggregatedMetricsIntegrationTest.getTimerDataToRoll(reader, locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(1, points.getPoints().size());
        
        // create the rollup
        final BluefloodTimerRollup rollup = BluefloodTimerRollup.buildRollupFromTimerRollups(points);
        // should be the same as simple
        Assert.assertEquals(simple, rollup);
        
        // assemble it into points, but give it a new timestamp.
        points = new Points<BluefloodTimerRollup>() {{
            add(new Point<BluefloodTimerRollup>(rollupTs, rollup));
        }};
        List<IMetric> toWrite = toIMetricsList(locator, points);
        writer.insertMetrics(toWrite, CassandraModel.CF_METRICS_PREAGGREGATED_5M);
        
        // we should be able to read that now.
        Points<BluefloodTimerRollup> rollups5m = reader.getDataToRoll(BluefloodTimerRollup.class, locator, new Range(rollupTs, rollupTs+1), CassandraModel.CF_METRICS_PREAGGREGATED_5M);
        
        Assert.assertEquals(1, rollups5m.getPoints().size());
        
        BluefloodTimerRollup rollup5m = rollups5m.getPoints().values().iterator().next().getData();
        // rollups should be identical since one is just a coarse rollup of the other.
        Assert.assertEquals(rollup, rollup5m);
    }
    
    @Test
    public void testTtlWorks() throws Exception {
        final long ts = timestamp.incrementAndGet();
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "ttl");
        IMetric metric = new PreaggregatedMetric(ts, locator, new TimeValue(2, TimeUnit.SECONDS), simple);
        
        // put it
        writer.insertMetrics(Lists.newArrayList(metric), CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
        
        // read it quickly.
        Points<BluefloodTimerRollup> points = PreaggregatedMetricsIntegrationTest.getTimerDataToRoll(reader, locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(1, points.getPoints().size());
        
        // let it time out.
        Thread.sleep(2000);
        
        // ensure it is gone.
        points = PreaggregatedMetricsIntegrationTest.getTimerDataToRoll(reader, locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(0, points.getPoints().size());
    }
    
    private static List<IMetric> toIMetricsList(Locator locator, Points<BluefloodTimerRollup> points) {
        List<IMetric> list = new ArrayList<IMetric>();
        for (Map.Entry<Long, Points.Point<BluefloodTimerRollup>> entry : points.getPoints().entrySet()) {
            PreaggregatedMetric metric = new PreaggregatedMetric(entry.getKey(), locator, ttl, entry.getValue().getData());
            list.add(metric);
        }
        return list;
    }
    
}
