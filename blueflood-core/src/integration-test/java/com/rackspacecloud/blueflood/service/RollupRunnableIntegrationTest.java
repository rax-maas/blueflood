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

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RollupRunnableIntegrationTest extends IntegrationTestBase {
    
    // gentle reader: remember, all column families are truncated between tests
    // in super.setUp()

    private AbstractMetricsRW basicRW = IOContainer.fromConfig().getBasicMetricsRW();
    private AbstractMetricsRW preAggrRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();

    
    private final Locator counterLocator = Locator.createLocatorFromPathComponents("runnabletest", "counter");
    private final Locator gaugeLocator = Locator.createLocatorFromPathComponents("runnabletest", "gauge");
    private final Locator timerLocator = Locator.createLocatorFromPathComponents("runnabletest", "timer");
    private final Locator setLocator = Locator.createLocatorFromPathComponents("runnabletest", "set");
    private final Locator enumLocator = Locator.createLocatorFromPathComponents("runnabletest", "enum");
    private final Locator normalLocator = Locator.createLocatorFromPathComponents("runnabletest", "just_some_data");
    
    private final Range range = new Range(0, 6 * 60 * 1000);
    
    private MetadataCache cache;

    @Before
    public void generateData() throws Exception {

        final TimeValue ttl = new TimeValue(24, TimeUnit.HOURS);
        
        // cache needs to be populated so rollups knows which serializer to use.
        cache = MetadataCache.createLoadingCacheInstance(ttl, 2);
        String cacheKey = MetricMetadata.ROLLUP_TYPE.name().toLowerCase();
        cache.put(counterLocator, cacheKey, RollupType.COUNTER.name());
        cache.put(gaugeLocator, cacheKey, RollupType.GAUGE.name());
        cache.put(timerLocator, cacheKey, RollupType.TIMER.name());
        cache.put(setLocator, cacheKey, RollupType.SET.name());
        cache.put(enumLocator, cacheKey, RollupType.ENUM.name());
        // do not put normalLocator in the cache. it will constitute a miss.
        
        // put some full resolution data.
        Collection<IMetric> preaggregatedMetrics = new ArrayList<IMetric>();
        Collection<IMetric> normalMetrics = new ArrayList<IMetric>();
        
        for (int i = 1; i <= 5; i++) {
            long time = i * 30000;
            IMetric metric;
            
            BluefloodCounterRollup counter = new BluefloodCounterRollup()
                    .withCount(i)
                    .withRate(i * i)
                    .withSampleCount(1);
            metric = new PreaggregatedMetric(time, counterLocator, ttl, counter);
            preaggregatedMetrics.add(metric);

            BluefloodGaugeRollup gauge = new BluefloodGaugeRollup()
                    .withLatest(time, i);
            metric = new PreaggregatedMetric(time, gaugeLocator, ttl, gauge);
            preaggregatedMetrics.add(metric);
            
            BluefloodTimerRollup timer = new BluefloodTimerRollup()
                    .withCount(5 * i + 1)
                    .withMaxValue(100 - i)
                    .withMinValue(100 - i - i)
                    .withAverage(i / 2)
                    .withCountPS((double)i).withSum(Double.valueOf(2 * i))
                    .withVariance((double) i / 2d);
            metric = new PreaggregatedMetric(time, timerLocator, ttl, timer);
            preaggregatedMetrics.add(metric);
            
            BluefloodSetRollup rollup = new BluefloodSetRollup().withObject(i);
            metric = new PreaggregatedMetric(time, setLocator, ttl, rollup);
            preaggregatedMetrics.add(metric);

            BluefloodEnumRollup enumRollup = new BluefloodEnumRollup().withHashedEnumValue((long)i, (long)5*(i+1));
            metric = new PreaggregatedMetric(time, enumLocator, ttl, enumRollup);
            preaggregatedMetrics.add(metric);

            metric = new Metric(normalLocator, i, time, ttl, "centipawns");
            normalMetrics.add(metric);
        }
        
        preAggrRW.insertMetrics(preaggregatedMetrics);
        basicRW.insertMetrics(normalMetrics);
         
    }
    
    @Test
    public void testNormalMetrics() throws IOException {
        // full res has 5 samples.
        Range customRange = new Range(0, 10 * 60 * 1000);
        Points points = basicRW.getDataToRollup(
                normalLocator,
                RollupType.NOT_A_ROLLUP,
                customRange,
                CassandraModel.CF_METRICS_FULL_NAME);
        System.out.println(String.format("points=%s", points.getPoints().toString()));
        Assert.assertEquals(String.format("Full res points for locator=%s range=%s", normalLocator, range), 5,
                basicRW.getDataToRollup(
                        normalLocator,
                        RollupType.NOT_A_ROLLUP,
                        range,
                        CassandraModel.CF_METRICS_FULL_NAME).getPoints().size());
        
        // assert nothing in 5m for this locator.
        Assert.assertEquals("5m res points", 0,
                basicRW.getDataToRollup(
                        normalLocator,
                        RollupType.BF_BASIC,
                        range,
                        CassandraModel.CF_METRICS_5M_NAME).getPoints().size());
        
        RollupExecutionContext rec = new RollupExecutionContext(Thread.currentThread());
        SingleRollupReadContext rc = new SingleRollupReadContext(normalLocator, range, Granularity.MIN_5);
        RollupBatchWriter batchWriter = new RollupBatchWriter(new ThreadPoolBuilder().build(), rec);
        RollupRunnable rr = new RollupRunnable(rec, rc, batchWriter, null);
        rr.run();

        while (!rec.doneReading() && !rec.doneWriting()) {
            batchWriter.drainBatch();
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
            }
        }

        // assert something in 5m for this locator.
        Assert.assertEquals(1, basicRW.getDataToRollup(
                                                    normalLocator,
                                                    RollupType.BF_BASIC,
                                                    range,
                                                    CassandraModel.CF_METRICS_5M_NAME).getPoints().size());
    }
    
    @Test
    public void testCounterRollup() throws IOException {
        testRolledupMetric(counterLocator, RollupType.COUNTER);
    }
    
    @Test
    public void testGaugeRollup() throws IOException {
        testRolledupMetric(gaugeLocator, RollupType.GAUGE);
    }
    
    @Test
    public void testTimerRollup() throws IOException {
        testRolledupMetric(timerLocator, RollupType.TIMER);
    }
    
    @Test
    public void testSetRollup() throws IOException {
        testRolledupMetric(setLocator, RollupType.SET);
    }

    @Test
    public void testEnumRollup() throws IOException {
        testRolledupMetric(enumLocator, RollupType.ENUM);
    }
    
    private void testRolledupMetric(Locator locator, RollupType rollupType) throws IOException {
        System.out.println("testRolledupMetric: for locator = " + locator + ", rollupTYpe=" + rollupType);
        // full res has 5 samples.
        Assert.assertEquals(5, preAggrRW.getDataToRollup(
                                                    locator,
                                                    rollupType,
                                                    range, 
                                                    CassandraModel.CF_METRICS_PREAGGREGATED_FULL_NAME).getPoints().size());
        
        // assert nothing in 5m for this locator.
        Assert.assertEquals(0, preAggrRW.getDataToRollup(
                                                    locator,
                                                    rollupType,
                                                    range, 
                                                    CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME).getPoints().size());
        
        RollupExecutionContext rec = new RollupExecutionContext(Thread.currentThread());
        SingleRollupReadContext rc = new SingleRollupReadContext(locator, range, Granularity.MIN_5);
        RollupBatchWriter batchWriter = new RollupBatchWriter(new ThreadPoolBuilder().build(), rec);
        ThreadPoolExecutor enumValidatorExec = mock(ThreadPoolExecutor.class);
        RollupRunnable rr = new RollupRunnable(rec, rc, batchWriter, enumValidatorExec);
        rr.run();

        if (rollupType == RollupType.ENUM) {
            verify(enumValidatorExec, times(1)).execute(any(EnumValidator.class));
        }

        // assert something in 5m for this locator.
        while (!rec.doneReading() && !rec.doneWriting()) {
            batchWriter.drainBatch();
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
            }
        }

        Assert.assertEquals(1, preAggrRW.getDataToRollup(
                                                    locator,
                                                    rollupType,
                                                    range,
                                                    CassandraModel.CF_METRICS_PREAGGREGATED_5M_NAME).getPoints().size());
    }
}
