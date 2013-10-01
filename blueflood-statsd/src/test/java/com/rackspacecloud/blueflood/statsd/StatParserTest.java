package com.rackspacecloud.blueflood.statsd;

import junit.framework.Assert;
import org.junit.Test;

public class StatParserTest { 
    
    @Test
    public void testDoubleGrok() {
        Assert.assertEquals(21.3456d, Stat.grokValue("21.3456"));
    }
    
    @Test
    public void testLongGrok() {
        Assert.assertEquals(12345l, Stat.grokValue("12345"));
    }
    
    @Test
    public void testStatsdLabelParsing() {
        String internalLabels[] = new String[] {
                "stats.some_prefix.bad_lines_seen",
                "stats.some_prefix.packets_received",
                "stats_counts.my_prefix.bad_lines_seen",
                "stats_counts.my_prefix.packets_received",
                "gentle_prefix.numStats",
                "stats.prefix.processing_time",
                "stats.prefix.graphiteStats.calculationtime",
                "stats.prefix.graphiteStats.last_exception",
                "stats.prefix.graphiteStats.last_flush",
                "stats.prefix.graphiteStats.flush_time",
                "stats.prefix.graphiteStats.flush_length",
        };
        
        for (String internal : internalLabels)
            Assert.assertTrue(internal, Stat.isInternal(internal));
        
        String externalLabels[] = new String[] {
                "stats.timers.this.is_my.timer.name.mean_90",
                "stats.gauges.mine",
                "stats.sets.my.set.name",
                "stats_counts.my.counter",
                "stats.my.counter",
        };
        
        for (String external : externalLabels)
            Assert.assertFalse(external, Stat.isInternal(external));
    }
}
