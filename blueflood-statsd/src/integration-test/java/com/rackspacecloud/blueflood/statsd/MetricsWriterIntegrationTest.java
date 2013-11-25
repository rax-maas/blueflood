package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.statsd.containers.StatsCollection;
import com.rackspacecloud.blueflood.statsd.containers.TypedMetricsCollection;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.annotation.ExceptionProxy;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class MetricsWriterIntegrationTest {
    
    private MetricsWriter writer;
    private Thread.UncaughtExceptionHandler exceptionHandler;
    private RejectedExecutionHandler rejectedHandler;
    StatsCollection stats;
    
    private int exceptionCount;
    private int rejectionCount;
    
    @Before
    public void setupWriter() {
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
        
        stats = new StatsCollection();
    }
    
    @Test
    public void testCounter() throws Exception {
        stats.add(new Stat("stats_counts.counter_0", 32, System.currentTimeMillis() / 1000));
        TypedMetricsCollection metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.getPreaggregatedMetrics().size());
    }
    
    @Test
    public void testBadCounter() throws Exception {
        stats.add(new Stat("malformed_counter_name.counter_0", 32, System.currentTimeMillis() / 1000));
        TypedMetricsCollection metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(0, metrics.getPreaggregatedMetrics().size());
    }
    
    @Test
    public void testGauge() throws Exception {
        stats.add(new Stat("stats.gauges.test_gauge_0", 45, System.currentTimeMillis() / 1000));
        TypedMetricsCollection metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.getPreaggregatedMetrics().size());
        
    }
    
    @Test
    public void testSet() throws Exception {
        stats.add(new Stat("stats.sets.test_set_0", 231, System.currentTimeMillis() / 1000));
        TypedMetricsCollection metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.getPreaggregatedMetrics().size());
    }
    
    @Test
    public void testTimer() throws Exception {
        for (String line : ConversionsTest.TIMER_LINES)
            stats.add(Conversions.asStat(line));
        TypedMetricsCollection metrics = writer.apply(stats).get();
        Assert.assertEquals(0, exceptionCount);
        Assert.assertEquals(0, rejectionCount);
        Assert.assertEquals(1, metrics.getPreaggregatedMetrics().size());
    }
    
    @Test
    public void testCombined() {
        
    }
}
