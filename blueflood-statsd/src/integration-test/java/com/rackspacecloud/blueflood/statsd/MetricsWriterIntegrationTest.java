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

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class MetricsWriterIntegrationTest extends IntegrationTestBase {
    
    private MetricsWriter writer;
    private Thread.UncaughtExceptionHandler exceptionHandler;
    private RejectedExecutionHandler rejectedHandler;
    StatCollection stats;
    
    private int exceptionCount;
    private int rejectionCount;
    
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
        
        stats = new StatCollection();
    }
    
    @Test
    public void testCounter() throws Exception {
        stats.add(new Stat("stats_counts.counter_0", 32, System.currentTimeMillis() / 1000));
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.COUNTER).size());
    }
    
    @Test
    public void testBadCounter() throws Exception {
        stats.add(new Stat("malformed_counter_name.counter_0", 32, System.currentTimeMillis() / 1000));
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(0, metrics.get(StatType.COUNTER).size());
    }
    
    @Test
    public void testGauge() throws Exception {
        stats.add(new Stat("stats.gauges.test_gauge_0", 45, System.currentTimeMillis() / 1000));
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.GAUGE).size());
        
    }
    
    @Test
    public void testSet() throws Exception {
        stats.add(new Stat("stats.sets.test_set_0", 231, System.currentTimeMillis() / 1000));
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.SET).size());
    }
    
    @Test
    public void testTimer() throws Exception {
        for (String line : ConversionsTest.TIMER_LINES)
            stats.add(Conversions.asStat(line));
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.get(StatType.TIMER).size());
    }
    
    @Test
    public void testCombined() throws Exception {
        // these four lines are a counter, bad counter, gauge and set.
        stats.add(new Stat("stats_counts.counter_0", 32, System.currentTimeMillis() / 1000));
        stats.add(new Stat("malformed_counter_name.counter_0", 32, System.currentTimeMillis() / 1000));
        stats.add(new Stat("stats.gauges.test_gauge_0", 45, System.currentTimeMillis() / 1000));
        stats.add(new Stat("stats.sets.test_set_0", 231, System.currentTimeMillis() / 1000));
        
        // these next lines constitute a single timer.
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean_99", 47.97651006711409, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper_99", 97, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum_99", 14297, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean_75", 36.469026548672566, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper_75", 72, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum_75", 8242, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean_50", 24.112582781456954, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper_50", 49, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum_50", 3641, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean_25", 12.16, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper_25", 24, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum_25", 912, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean_1", 0.6666666666666666, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper_1", 1, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum_1", 2, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.std", 28.133704832870738, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.upper", 99, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.lower", 0, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.count", 301, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.count_ps", 10.033333333333333, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.sum", 14592, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.mean", 48.478405315614616, 1380825845));
        stats.add(new Stat("stats.timers.gary.foo.bar.timer.median", 49, 1380825845));
        
        Multimap<StatType, IMetric> metrics = writer.apply(stats).get();
        
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        
        Assert.assertEquals(1, metrics.get(StatType.GAUGE).size());
        Assert.assertEquals(1, metrics.get(StatType.COUNTER).size());
        Assert.assertEquals(1, metrics.get(StatType.SET).size());
        Assert.assertEquals(1, metrics.get(StatType.TIMER).size());
        Assert.assertEquals(0, metrics.get(StatType.UNKNOWN).size());
    }
}
