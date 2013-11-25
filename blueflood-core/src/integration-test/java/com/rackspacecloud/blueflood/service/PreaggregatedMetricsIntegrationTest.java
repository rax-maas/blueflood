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
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PreaggregatedMetricsIntegrationTest extends IntegrationTestBase {
    
    private TimerRollup simple;
    private static final TimeValue ttl = new TimeValue(24, TimeUnit.HOURS);
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    private AstyanaxReader reader = AstyanaxReader.getInstance();
    private static final AtomicLong timestamp = new AtomicLong(10);
    
    @Before
    public void createFixtures() throws Exception {
        simple = new TimerRollup()
            .withSum(100L)
            .withCountPS(101d)
            .withAverage(102L)
            .withVariance(103d)
            .withMinValue(104)
            .withMaxValue(105)
            .withCount(106);
        simple.setPercentile("98th", 107, 108, 109);
        simple.setPercentile("99th", 110, 111, 112);
        // todo: add a few percentiles.
    }
    
    @Test
    public void testFullReadWrite() throws Exception {
        long ts = timestamp.incrementAndGet();
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "full", "read", "write");
        IMetric metric = new PreaggregatedMetric(ts, locator, ttl, simple);

        writer.insertMetrics(Lists.newArrayList(metric), AstyanaxIO.CF_METRICS_PREAGGREGATED);
        
        Points<TimerRollup> points = reader.getTimerDataToRoll(locator, new Range(ts, ts+1), Granularity.FULL);

        Assert.assertEquals(1, points.getPoints().size());
        Assert.assertEquals(metric.getValue(), points.getPoints().get(ts).getData());
    }
    
    @Test
    public void testHigherGranReadWrite() throws Exception {
        final long ts = timestamp.incrementAndGet();
        final long rollupTs = ts + 100;
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "rollup", "read", "write");
        IMetric metric = new PreaggregatedMetric(ts, locator, ttl, simple);
        
        writer.insertMetrics(Lists.newArrayList(metric), AstyanaxIO.CF_METRICS_PREAGGREGATED);
        
        // read the raw data.
        Points<TimerRollup> points = reader.getTimerDataToRoll(locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(1, points.getPoints().size());
        
        // create the rollup
        final TimerRollup rollup = TimerRollup.buildRollupFromTimerRollups(points);
        // should be the same as simple
        Assert.assertEquals(simple, rollup);
        
        // assemble it into points, but give it a new timestamp.
        points = new Points<TimerRollup>() {{
            add(new Point<TimerRollup>(rollupTs, rollup));
        }};
        List<IMetric> toWrite = toIMetricsList(locator, points);
        writer.insertMetrics(toWrite, AstyanaxIO.CF_METRICS_5M);
        
        // we should be able to read that now.
        Points<TimerRollup> rollups5m = reader.getDataToRoll(TimerRollup.class, locator, new Range(rollupTs, rollupTs+1), AstyanaxIO.CF_METRICS_5M);
        
        Assert.assertEquals(1, rollups5m.getPoints().size());
        
        TimerRollup rollup5m = rollups5m.getPoints().values().iterator().next().getData();
        // rollups should be identical since one is just a coarse rollup of the other.
        Assert.assertEquals(rollup, rollup5m);
    }
    
    @Test
    public void testTtlWorks() throws Exception {
        final long ts = timestamp.incrementAndGet();
        Locator locator = Locator.createLocatorFromPathComponents("12345", "test", "ttl");
        IMetric metric = new PreaggregatedMetric(ts, locator, new TimeValue(2, TimeUnit.SECONDS), simple);
        
        // write it
        writer.insertMetrics(Lists.newArrayList(metric), AstyanaxIO.CF_METRICS_PREAGGREGATED);
        
        // read it quickly.
        Points<TimerRollup> points = reader.getTimerDataToRoll(locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(1, points.getPoints().size());
        
        // let it time out.
        Thread.sleep(2000);
        
        // ensure it is gone.
        points = reader.getTimerDataToRoll(locator, new Range(ts, ts+1), Granularity.FULL);
        Assert.assertEquals(0, points.getPoints().size());
    }
    
    private static List<IMetric> toIMetricsList(Locator locator, Points<TimerRollup> points) {
        List<IMetric> list = new ArrayList<IMetric>();
        for (Map.Entry<Long, Points.Point<TimerRollup>> entry : points.getPoints().entrySet()) {
            PreaggregatedMetric metric = new PreaggregatedMetric(entry.getKey(), locator, ttl, entry.getValue().getData());
            list.add(metric);
        }
        return list;
    }
    
}
