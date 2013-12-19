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

package com.rackspacecloud.blueflood.statsd;

import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.StatType;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

@RunWith(value = Parameterized.class)
public class MetricsWriterIntegrationTest extends IntegrationTestBase {
    
    private MetricsWriter writer;
    private Thread.UncaughtExceptionHandler exceptionHandler;
    private RejectedExecutionHandler rejectedHandler;
    private StatCollection statsToWrite;
    
    private StatCollection statsSource;
    
    private int exceptionCount;
    private int rejectionCount;
    
    private final File src;
    private final StatsdOptions options;
    
    
    public MetricsWriterIntegrationTest(String path, StatsdOptions options) {
        this.src = new File(path);
        this.options = options;
    }
    
    @Before
    public void setUp() throws Exception {
        super.setUp();
        
        exceptionHandler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                exceptionCount += 1;
            }
        };
        
        rejectedHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                rejectionCount += 1;
            }
        };
        
        writer = new MetricsWriter(new ThreadPoolBuilder()
            .withUnboundedQueue()
            .withMaxPoolSize(5)
            .withCorePoolSize(1)
            .withName("test writer")
            .withExeptionHandler(exceptionHandler)
            .withRejectedHandler(rejectedHandler)
            .build());
        
        statsToWrite = new StatCollection();
        statsSource = readStats(src, options);
        
        
    }
    
    private static StatCollection readStats(File path, StatsdOptions options) throws IOException {
        String[] lines = StatParserTest.readStatLines(path);
        StatCollection stats = new StatCollection();
        for (String line : lines) {
            try {
                stats.add(Conversions.asStat(line, options));
            } catch (Throwable th) {
                Assert.fail(String.format("ERR:%s, LINE:%s, PATH:%s", th.getMessage(), line, path));
            }
        }
        StatCollection.renderTimersAsCounters(stats);
        return stats;
    }
    
    @Test
    public void testCounter() throws Exception {
        for (Stat counterPart : statsSource.getStats(StatType.COUNTER))
            statsToWrite.add(counterPart);
        Multimap<StatType, IMetric> metrics = writer.apply(statsToWrite).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(3, metrics.get(StatType.COUNTER).size());
    }
    
    @Test
    public void testGauge() throws Exception {
        statsToWrite.add(statsSource.getStats(StatType.GAUGE).iterator().next());
        Multimap<StatType, IMetric> metrics = writer.apply(statsToWrite).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.GAUGE).size());
        
    }
    
    @Test
    public void testSet() throws Exception {
        statsToWrite.add(statsSource.getStats(StatType.SET).iterator().next());
        Multimap<StatType, IMetric> metrics = writer.apply(statsToWrite).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.SET).size());
    }
    
    @Test
    public void testTimer() throws Exception {
        for (Stat timerPart : statsSource.getStats(StatType.TIMER))
            statsToWrite.add(timerPart);
        Multimap<StatType, IMetric> metrics = writer.apply(statsToWrite).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.TIMER).size());
    }
    
    @Test
    public void testCombined() throws Exception {
        Multimap<StatType, IMetric> metrics = writer.apply(statsSource).get();
        
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        
        Assert.assertEquals(10, metrics.get(StatType.GAUGE).size());
        Assert.assertEquals(3, metrics.get(StatType.COUNTER).size());
        Assert.assertEquals(1, metrics.get(StatType.SET).size());
        Assert.assertEquals(1, metrics.get(StatType.TIMER).size());
        Assert.assertEquals(0, metrics.get(StatType.UNKNOWN).size());
    }
    
    @Parameterized.Parameters
    public static Collection constructionParameters() throws IOException {
        Object[][] params = new Object[][] {
                StatParserTest.getLegacyNoPrefixOrSuffix(),
                StatParserTest.getLegacyWithPrefixAndSuffix(),
                StatParserTest.getModernNoPrefixOrSuffix(),
                StatParserTest.getModernWithPrefix()
        };
        return Arrays.asList(params);
    }
}
