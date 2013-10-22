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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatsCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.statsd.containers.StatType;
import com.rackspacecloud.blueflood.statsd.containers.TypedMetricsCollection;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.TimerRollup;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

public class ConversionsTest {
    
    public static final String[] METRIC_LINES = new String[] {
            "stats.myprefix.bad_lines_seen 0 1380825845", // unk = 1
            "stats_counts.myprefix.bad_lines_seen 0 1380825845", // cnt = 1 
            "stats.myprefix.packets_received 0.5333333333333333 1380825845", // unk = 2
            "stats_counts.myprefix.packets_received 16 1380825845", // cnt = 2
            "stats_counts.gary.foo.bar.counter 301 1380825845", // cnt = 3
            "stats.timers.gary.foo.bar.timer.mean_99 47.97651006711409 1380825845", // tmr = 1
            "stats.timers.gary.foo.bar.timer.upper_99 97 1380825845",
            "stats.timers.gary.foo.bar.timer.sum_99 14297 1380825845",
            "stats.timers.gary.foo.bar.timer.mean_75 36.469026548672566 1380825845",
            "stats.timers.gary.foo.bar.timer.upper_75 72 1380825845",
            "stats.timers.gary.foo.bar.timer.sum_75 8242 1380825845",
            "stats.timers.gary.foo.bar.timer.mean_50 24.112582781456954 1380825845",
            "stats.timers.gary.foo.bar.timer.upper_50 49 1380825845",
            "stats.timers.gary.foo.bar.timer.sum_50 3641 1380825845",
            "stats.timers.gary.foo.bar.timer.mean_25 12.16 1380825845",
            "stats.timers.gary.foo.bar.timer.upper_25 24 1380825845",
            "stats.timers.gary.foo.bar.timer.sum_25 912 1380825845",
            "stats.timers.gary.foo.bar.timer.mean_1 0.6666666666666666 1380825845",
            "stats.timers.gary.foo.bar.timer.upper_1 1 1380825845",
            "stats.timers.gary.foo.bar.timer.sum_1 2 1380825845",
            "stats.timers.gary.foo.bar.timer.std 28.133704832870738 1380825845",
            "stats.timers.gary.foo.bar.timer.upper 99 1380825845",
            "stats.timers.gary.foo.bar.timer.lower 0 1380825845",
            "stats.timers.gary.foo.bar.timer.count 301 1380825845",
            "stats.timers.gary.foo.bar.timer.count_ps 10.033333333333333 1380825845",
            "stats.timers.gary.foo.bar.timer.sum 14592 1380825845",
            "stats.timers.gary.foo.bar.timer.mean 48.478405315614616 1380825845",
            "stats.timers.gary.foo.bar.timer.median 49 1380825845", // tmr = 23
            "stats.gauges.gary.foo.bar.gauge 5517 1380825845", // gag = 1
            "stats.sets.gary.foo.bar.set.count 11 1380825845", // set = 1
            "myprefix.numStats 6 1380825845", // unk = 3
            "stats.myprefix.graphiteStats.calculationtime 0 1380825845", // unk = 4
            "stats.myprefix.processing_time 0 1380825845", // unk = 5
            "stats.myprefix.graphiteStats.last_exception 1380815339 1380825845", // unk = 6
            "stats.myprefix.graphiteStats.last_flush 1380825665 1380825845", // unk = 7
            "stats.myprefix.graphiteStats.flush_time 0 1380825845", // unk = 8
            "stats.myprefix.graphiteStats.flush_length 2085 1380825845" // unk = 9
        };
    
    public static final String[] TIMER_LINES = new String[] {
            "stats.timers.gary.foo.bar.mean_99 47.97651006711409 1380825845",
            "stats.timers.gary.foo.bar.upper_99 97 1380825845",
            "stats.timers.gary.foo.bar.sum_99 14297 1380825845",
            "stats.timers.gary.foo.bar.mean_75 36.469026548672566 1380825845",
            "stats.timers.gary.foo.bar.upper_75 72 1380825845",
            "stats.timers.gary.foo.bar.sum_75 8242 1380825845",
            "stats.timers.gary.foo.bar.mean_50 24.112582781456954 1380825845",
            "stats.timers.gary.foo.bar.upper_50 49 1380825845",
            "stats.timers.gary.foo.bar.sum_50 3641 1380825845",
            "stats.timers.gary.foo.bar.mean_25 12.16 1380825845",
            "stats.timers.gary.foo.bar.upper_25 24 1380825845",
            "stats.timers.gary.foo.bar.sum_25 912 1380825845",
            "stats.timers.gary.foo.bar.mean_1 0.6666666666666666 1380825845",
            "stats.timers.gary.foo.bar.upper_1 1 1380825845",
            "stats.timers.gary.foo.bar.sum_1 2 1380825845",
            "stats.timers.gary.foo.bar.std 28.133704832870738 1380825845",
            "stats.timers.gary.foo.bar.upper 99 1380825845",
            "stats.timers.gary.foo.bar.lower 0 1380825845",
            "stats.timers.gary.foo.bar.count 301 1380825845",
            "stats.timers.gary.foo.bar.count_ps 10.033333333333333 1380825845",
            "stats.timers.gary.foo.bar.sum 14592 1380825845",
            "stats.timers.gary.foo.bar.mean 48.478405315614616 1380825845",
            "stats.timers.gary.foo.bar.median 49 1380825845"
    };
    
    @Test
    public void testTypes() {
        Multimap<StatType, Object> counts = LinkedListMultimap.create();
        for (String line : METRIC_LINES) {
            Stat stat = Conversions.asStat(line);
            Assert.assertNotNull(stat);
            counts.get(stat.getType()).add(new Object());
        }
        
        // ensure total count is accurate
        int sum = 0;
        for (Map.Entry<StatType, Collection<Object>> entry : counts.asMap().entrySet())
            sum += entry.getValue().size();
        Assert.assertEquals(METRIC_LINES.length, sum);
        
        // ensure individual count is accurate.
        Assert.assertEquals(9, counts.get(StatType.UNKNOWN).size());
        Assert.assertEquals(3, counts.get(StatType.COUNTER).size());
        Assert.assertEquals(23, counts.get(StatType.TIMER).size());
        Assert.assertEquals(1, counts.get(StatType.GAUGE).size());
        Assert.assertEquals(1, counts.get(StatType.SET).size());
    }
    
    @Test
    public void testStatToMetricNormal() {
        final String [] lines = new String[] {
                "stats.myprefix.bad_lines_seen 0 1380825845",
                "stats.myprefix.packets_received 0.5333333333333333 1380825845", // unk = 2
                "myprefix.numStats 6 1380825845", // unk = 3
                "stats.myprefix.graphiteStats.calculationtime 0 1380825845", // unk = 4
                "stats.myprefix.processing_time 0 1380825845", // unk = 5
                "stats.myprefix.graphiteStats.last_exception 1380815339 1380825845", // unk = 6
                "stats.myprefix.graphiteStats.last_flush 1380825665 1380825845", // unk = 7
                "stats.myprefix.graphiteStats.flush_time 0 1380825845", // unk = 8
                "stats.myprefix.graphiteStats.flush_length 2085 1380825845" // unk = 9
        };
        
        StatsCollection stats = ConversionsTest.asStats(lines);
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        
        Assert.assertEquals(9, metrics.getNormalMetrics().size());
        Assert.assertEquals(0, metrics.getPreaggregatedMetrics().size());
        
        // 1 double and 8 longs.
    }
    
    @Test
    public void testStatToMetricCounters() {
        final String[] lines = new String[] {
                "stats_counts.myprefix.bad_lines_seen 0 1380825845",
                "stats_counts.myprefix.packets_received 16 1380825845",
                "stats_counts.gary.foo.bar.counter 301 1380825845"
        };
        
        StatsCollection stats = ConversionsTest.asStats(lines);
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        
        Assert.assertEquals(0, metrics.getNormalMetrics().size());
        Assert.assertEquals(3, metrics.getPreaggregatedMetrics().size());
        for (PreaggregatedMetric metric : metrics.getPreaggregatedMetrics())
            Assert.assertTrue(metric.getValue() instanceof CounterRollup);
    }
    
    @Test
    public void testStatToMetricSet() {
        final String[] lines = new String[] {
                "stats.sets.gary.foo.bar.A 11 1380825845",
                "stats.sets.foo.bar.B 45 1380825845",
                "stats.sets.bar.C 21 1380825845",
        };
        
        StatsCollection stats = ConversionsTest.asStats(lines);
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        
        Assert.assertEquals(0, metrics.getNormalMetrics().size());
        Assert.assertEquals(3, metrics.getPreaggregatedMetrics().size());
        for (PreaggregatedMetric metric : metrics.getPreaggregatedMetrics())
            Assert.assertTrue(metric.getValue() instanceof SetRollup);
    }
    
    @Test
    public void testStatToMetricGauge() {
        final String[] lines = new String[] {
                "stats.gauges.gary.foo.bar.A 5517 1380825845",
                "stats.gauges.foo.bar.B 5517 1380825845",
                "stats.gauges.bar.C 5517 1380825845",
        };
        
        StatsCollection stats = ConversionsTest.asStats(lines);
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        
        Assert.assertEquals(0, metrics.getNormalMetrics().size());
        Assert.assertEquals(3, metrics.getPreaggregatedMetrics().size());
        for (PreaggregatedMetric metric : metrics.getPreaggregatedMetrics())
            Assert.assertTrue(metric.getValue() instanceof GaugeRollup);
    }
    
    @Test
    public void testStatToMetricTimer() {
        StatsCollection stats = ConversionsTest.asStats(TIMER_LINES);
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        
        Assert.assertEquals(0, metrics.getNormalMetrics().size());
        Assert.assertEquals(1, metrics.getPreaggregatedMetrics().size());
        for (PreaggregatedMetric metric : metrics.getPreaggregatedMetrics())
            Assert.assertTrue(metric.getValue() instanceof TimerRollup);
    }
    
    @Test
    public void testAllStatToMetric() {
        StatsCollection stats = ConversionsTest.asStats(METRIC_LINES);

        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        Assert.assertEquals(9, metrics.getNormalMetrics().size());
        Assert.assertEquals(6, metrics.getPreaggregatedMetrics().size());
        
        Multimap<Class, IMetric> rollups = HashMultimap.create();
        for (IMetric metric : metrics.getNormalMetrics())
            rollups.put(metric.getValue().getClass(), metric);
        for (IMetric metric : metrics.getPreaggregatedMetrics())
            rollups.put(metric.getValue().getClass(), metric);
        
        Assert.assertEquals(3, rollups.get(CounterRollup.class).size());
        Assert.assertEquals(1, rollups.get(TimerRollup.class).size()); // condensed from 23.
        Assert.assertEquals(1, rollups.get(GaugeRollup.class).size());
        Assert.assertEquals(1, rollups.get(SetRollup.class).size());
        
        Assert.assertEquals(1, rollups.get(Double.class).size());
        Assert.assertEquals(8, rollups.get(Long.class).size());
    }
    
    @Test
    public void testInvalidCounterConversion() {
        StatsCollection stats = new StatsCollection();
        stats.add(new Stat("malformed_counter_name.counter_0", 32, System.currentTimeMillis() / 1000));
        TypedMetricsCollection metrics = Conversions.asMetrics(stats);
        Assert.assertEquals(0, metrics.getNormalMetrics().size());
        Assert.assertEquals(0, metrics.getPreaggregatedMetrics().size());
    }
    
    private static StatsCollection asStats(String[] lines) {
        StatsCollection stats = new StatsCollection();
        for (String line : lines)
            stats.add(Conversions.asStat(line));
        return stats;
    }
}
