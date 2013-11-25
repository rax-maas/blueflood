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

import com.rackspacecloud.blueflood.statsd.containers.Stat;
import junit.framework.Assert;
import org.junit.Test;

public class StatParserTest { 
    
    @Test
    public void testDoubleGrok() {
        Assert.assertEquals(21.3456d, Stat.Parser.grokValue("21.3456"));
    }
    
    @Test
    public void testLongGrok() {
        Assert.assertEquals(12345l, Stat.Parser.grokValue("12345"));
    }
    
    @Test
    public void testLeftShift() {
        String[] leftShift = new String[] { "one", "two", "three"};
        leftShift = Stat.Parser.shiftLeft(leftShift, 1);
        Assert.assertEquals(2, leftShift.length);
        Assert.assertEquals("two", leftShift[0]);
        Assert.assertEquals("three", leftShift[1]);
    }
    
    @Test
    public void testRightShift() {
        String[] rightShift = new String[] { "one", "two", "three"};
        rightShift = Stat.Parser.shiftRight(rightShift, 1);
        Assert.assertEquals(2, rightShift.length);
        Assert.assertEquals("one", rightShift[0]);
        Assert.assertEquals("two", rightShift[1]);
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
            Assert.assertTrue(internal, Stat.Parser.isInternal(internal));
        
        String externalLabels[] = new String[] {
                "stats.timers.this.is_my.timer.name.mean_90",
                "stats.gauges.mine",
                "stats.sets.my.set.name",
                "stats_counts.my.counter",
                "stats.my.counter",
        };
        
        for (String external : externalLabels)
            Assert.assertFalse(external, Stat.Parser.isInternal(external));
    }
}
