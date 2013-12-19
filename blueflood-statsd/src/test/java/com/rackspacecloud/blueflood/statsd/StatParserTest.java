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
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.statsd.containers.StatLabel;
import com.rackspacecloud.blueflood.types.StatType;
import com.rackspacecloud.blueflood.types.IMetric;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class StatParserTest {
    
    private final File src;
    private final StatsdOptions options;
    
    public StatParserTest(String path, StatsdOptions options) {
        this.src = new File(path);
        this.options = options;
    }
    
    public static String[] readStatLines(File path) {
        Assert.assertTrue("File does not exist: " + path.getAbsolutePath(), path.exists());
        List<String> lines = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            try {
                String line = reader.readLine();
                
                while (line != null) {
                    line = line.trim();
                    if (line.length() > 0 && !line.startsWith("#"))
                        lines.add(line);
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException ex) {
                Assert.fail(ex.getMessage());
            }
        } catch (FileNotFoundException ex) {
            Assert.fail(ex.getMessage());
        }
        return lines.toArray(new String[lines.size()]);
    }
    
    @Test
    public void testLineConversion() {
        String[] lines = readStatLines(src);
        
        Assert.assertTrue(lines.length > 0);
        
        for (String line : lines) {
            Stat stat = null;
            try {
                stat = Conversions.asStat(line, options);
            } catch (Throwable th) {
                Assert.fail("BUSTED: " + line);
            }
            Assert.assertNotNull(stat);
        }
    }
    
    @Test
    public void testTypes() {
        String[] lines = readStatLines(src);
        StatCollection stats = StatParserTest.asStats(lines, options);
        StatCollection.renderTimersAsCounters(stats);
        
        // ensure individual count is accurate.
        
        // at one point, we sent the rate component of a counter in as UNKNOWN, but that is no longer the case.
        Assert.assertEquals(0, stats.getStats(StatType.UNKNOWN).size());
        
        Assert.assertEquals(6, stats.getStats(StatType.COUNTER).size());
        Assert.assertEquals(23, stats.getStats(StatType.TIMER).size());
        Assert.assertEquals(10, stats.getStats(StatType.GAUGE).size());
        Assert.assertEquals(1, stats.getStats(StatType.SET).size());
    }

    @Test
    public void testAllStatToMetric() {
        String[] lines = readStatLines(src);
        
        StatCollection stats = StatParserTest.asStats(lines, options);
        StatCollection.renderTimersAsCounters(stats);
        
        // values must be non-null.
        for (Stat stat : stats.iterableUnsafe())
            Assert.assertNotNull(stat.getLabel().toString(), stat.getValue());
        
        Multimap<StatType, IMetric> metrics = Conversions.asMetrics(stats);
        
        // total metrics
        Assert.assertEquals(15, metrics.size());
        
        // broken up this way.
        Assert.assertEquals(1, metrics.get(StatType.TIMER).size());
        Assert.assertEquals(1, metrics.get(StatType.SET).size());
        Assert.assertEquals(10, metrics.get(StatType.GAUGE).size());
        Assert.assertEquals(3, metrics.get(StatType.COUNTER).size());
        Assert.assertEquals(0, metrics.get(StatType.UNKNOWN).size());
            
        // all values must be null;
        for (IMetric metric : metrics.values())
            Assert.assertNotNull(metric.getLocator().toString(), metric.getValue());
    }
    
    private static StatCollection asStats(String[] lines, StatsdOptions options) {
        StatCollection stats = new StatCollection();
        for (String line : lines)
            stats.add(Conversions.asStat(line, options));
        return stats;
    }
    
    @Test
    public void testDoubleGrok() {
        Assert.assertEquals(21.3456d, Util.grokValue("21.3456"));
    }
    
    @Test
    public void testLongGrok() {
        Assert.assertEquals(12345l, Util.grokValue("12345"));
    }
    
    @Test
    public void testLeftShift() {
        String[] leftShift = new String[] { "one", "two", "three"};
        leftShift = Util.shiftLeft(leftShift, 1);
        Assert.assertEquals(2, leftShift.length);
        Assert.assertEquals("two", leftShift[0]);
        Assert.assertEquals("three", leftShift[1]);
    }
    
    @Test
    public void testRightShift() {
        String[] rightShift = new String[] { "one", "two", "three"};
        rightShift = Util.shiftRight(rightShift, 1);
        Assert.assertEquals(2, rightShift.length);
        Assert.assertEquals("one", rightShift[0]);
        Assert.assertEquals("two", rightShift[1]);
    }
    
    public static Object[] getLegacyNoPrefixOrSuffix() throws IOException {
        System.setProperty(StatsdConfig.STATSD_LEGACY_NAMESPACE.toString(), "true");
        System.clearProperty(StatsdConfig.STATSD_GLOBAL_PREFIX.toString());
        System.clearProperty(StatsdConfig.STATSD_GLOBAL_SUFFIX.toString());
        System.setProperty(StatsdConfig.STATSD_PREFIX_INTERNAL.toString(), "internal_prefix");
        System.setProperty(StatsdConfig.STATSD_PREFIX_COUNTER.toString(), "zzcounters");
        System.setProperty(StatsdConfig.STATSD_PREFIX_SET.toString(), "zzsets");
        System.setProperty(StatsdConfig.STATSD_PREFIX_TIMER.toString(), "zztimers");
        System.setProperty(StatsdConfig.STATSD_PREFIX_GAUGE.toString(), "zzgauges");
        Configuration config = Configuration.getInstance();
        config.init();
        
        return new Object[] {
                "src/test/resources/statsd_lines/legacy_no_global_prefix_or_suffix.txt",
                new StatsdOptions(config)
        };
    }
    
    public static Object[] getLegacyWithPrefixAndSuffix() throws IOException {
        System.setProperty(StatsdConfig.STATSD_LEGACY_NAMESPACE.toString(), "true");
        System.setProperty(StatsdConfig.STATSD_GLOBAL_PREFIX.toString(), "zzglobalprefox");
        System.setProperty(StatsdConfig.STATSD_GLOBAL_SUFFIX.toString(), "zzsufficks");
        System.setProperty(StatsdConfig.STATSD_PREFIX_INTERNAL.toString(), "internal_prefix");
        System.setProperty(StatsdConfig.STATSD_PREFIX_COUNTER.toString(), "zzcounters");
        System.setProperty(StatsdConfig.STATSD_PREFIX_SET.toString(), "zzsets");
        System.setProperty(StatsdConfig.STATSD_PREFIX_TIMER.toString(), "zztimers");
        System.setProperty(StatsdConfig.STATSD_PREFIX_GAUGE.toString(), "zzgauges");
        Configuration config = Configuration.getInstance();
        config.init();
        
        return new Object[] {
                "src/test/resources/statsd_lines/legacy_with_global_prefix_and_suffix.txt",
                new StatsdOptions(config)
        };
    }
    
    public static Object[] getModernNoPrefixOrSuffix() throws IOException {
        System.setProperty(StatsdConfig.STATSD_LEGACY_NAMESPACE.toString(), "false");
        System.clearProperty(StatsdConfig.STATSD_GLOBAL_PREFIX.toString());
        System.clearProperty(StatsdConfig.STATSD_GLOBAL_SUFFIX.toString());
        System.setProperty(StatsdConfig.STATSD_PREFIX_INTERNAL.toString(), "internal_prefix");
        System.setProperty(StatsdConfig.STATSD_PREFIX_COUNTER.toString(), "zzcounters");
        System.setProperty(StatsdConfig.STATSD_PREFIX_SET.toString(), "zzsets");
        System.setProperty(StatsdConfig.STATSD_PREFIX_TIMER.toString(), "zztimers");
        System.setProperty(StatsdConfig.STATSD_PREFIX_GAUGE.toString(), "zzgauges");
        Configuration config = Configuration.getInstance();
        config.init();
                
        return new Object[] {
                "src/test/resources/statsd_lines/modern_no_global_prefix_or_suffix.txt",
                new StatsdOptions(config)
        };
    }
    
    public static Object[] getModernWithPrefix() throws IOException {
        System.setProperty(StatsdConfig.STATSD_LEGACY_NAMESPACE.toString(), "false");
        System.setProperty(StatsdConfig.STATSD_GLOBAL_PREFIX.toString(), "zzglobalprefox");
        System.setProperty(StatsdConfig.STATSD_GLOBAL_SUFFIX.toString(), "zzsufficks");
        System.setProperty(StatsdConfig.STATSD_PREFIX_INTERNAL.toString(), "internal_prefix");
        System.setProperty(StatsdConfig.STATSD_PREFIX_COUNTER.toString(), "zzcounters");
        System.setProperty(StatsdConfig.STATSD_PREFIX_SET.toString(), "zzsets");
        System.setProperty(StatsdConfig.STATSD_PREFIX_TIMER.toString(), "zztimers");
        System.setProperty(StatsdConfig.STATSD_PREFIX_GAUGE.toString(), "zzgauges");
        Configuration config = Configuration.getInstance();
        config.init();
                
        return new Object[] {
                "src/test/resources/statsd_lines/modern_with_global_prefix.txt",
                new StatsdOptions(config)
        };
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
