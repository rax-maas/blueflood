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

package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.types.Average;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GranularityTest {

    final long baseMillis = 1335820166000L;

    @Test
    public void testFromPointsInInterval() throws Exception {
        Assert.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 86400).name());
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 288).name());
        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 72).name());
        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 24).name());
        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 6).name());
        Assert.assertEquals(Granularity.MIN_1440.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 1).name());

        Assert.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 800).name());
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 288).name()); // 144 5m points vs 1440 full points.
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 144).name());
        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 35).name());
        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 11).name());
        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 3).name());

        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(baseMillis, baseMillis + 86400000, 287).name());
    }

    @Test
    public void testRangesForInterval() throws Exception {
        Assert.assertEquals(1, countIterable(Range.rangesForInterval(Granularity.FULL, 0, 86399000)));
        Assert.assertEquals(288, countIterable(Range.rangesForInterval(Granularity.MIN_5, 0, 86399000)));
        Assert.assertEquals(72, countIterable(Range.rangesForInterval(Granularity.MIN_20, 0, 86399000)));
        Assert.assertEquals(24, countIterable(Range.rangesForInterval(Granularity.MIN_60, 0, 86399000)));
        Assert.assertEquals(6, countIterable(Range.rangesForInterval(Granularity.MIN_240, 0, 86399000)));
        Assert.assertEquals(1, countIterable(Range.rangesForInterval(Granularity.MIN_1440, 0, 86399000)));
        // The following test case was added after a production issue in which the call to rangesForInterval never
        // terminated.
        Assert.assertEquals(7, countIterable(Range.rangesForInterval(Granularity.MIN_240, System.currentTimeMillis() - (24 * 60 * 60 * 1000), System.currentTimeMillis())));
    }

    private int countIterable(Iterable<Range> ir) {
        int count = 0;
        for (Range r: ir) {
            count ++;
        }
        return count;
    }

    @Test
    public void testForCloseness() {
        int desiredPoints = 10;
        long start = 1335796192000L;
        // 10000000ms == 166.67 min.  166/20 is 8 points. 166/5 is 33 points.  The old algorithm returned the latter, which is too many.
        // 1000000ms == 16.67 min. 16/20 is 0, 16/5 is 3 points, 16/full = 32 points.
        // is too many.
        Assert.assertEquals(Granularity.MIN_20, Granularity.granularityFromPointsInInterval(start, start + 10000000, desiredPoints));
        Assert.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval(start, start + 1000000, desiredPoints));
      
        // test for points over a 100000000 millisecond swath. test round numbers as well as edge cases.  For reference 100k secs
        // generates:
        // 3333.33 full res points (at 30s per check)
        // 333.33 5min points
        // 83.33 20min points
        // 27.78 60min points
        // 6.94 240min points
        // 1.15 1440min points
        Map<Integer, Granularity> expectedGranularities = new HashMap<Integer, Granularity>() {{
            put(5000, Granularity.FULL);
            put(3332, Granularity.FULL);
            put(1835, Granularity.FULL);
            put(1832, Granularity.MIN_5);
            put(335, Granularity.MIN_5);
            put(332, Granularity.MIN_5);
            put(210, Granularity.MIN_5);
            put(206, Granularity.MIN_20);
            put(82, Granularity.MIN_20);
            put(57, Granularity.MIN_20);
            put(54, Granularity.MIN_60);
            put(30, Granularity.MIN_60);
            put(19, Granularity.MIN_60);
            put(16, Granularity.MIN_240);
            put(10, Granularity.MIN_240);
            put(5, Granularity.MIN_240);
            put(2, Granularity.MIN_1440);
            put(1, Granularity.MIN_1440);
        }};
        
        for (Map.Entry<Integer, Granularity> entry : expectedGranularities.entrySet()) {
            Granularity actual = Granularity.granularityFromPointsInInterval(0, 100000000, entry.getKey()); 
            Assert.assertEquals(
                    String.format("%d points", entry.getKey()),
                    entry.getValue(), 
                    actual);
        }
    }

    @Test(expected = GranularityException.class)
    public void testTooCoarse() throws Exception {
        Granularity g = Granularity.FULL;
        Granularity[] granularities = Granularity.granularities();

        int count = 1;
        while (true) {
            g = g.coarser();
            Assert.assertEquals(granularities[count++], g);
        }
    }

    @Test(expected = GranularityException.class)
    public void testTooFine() throws Exception {
        Granularity g = Granularity.MIN_1440;
        Granularity[] granularities = Granularity.granularities();

        int count = granularities.length - 2;

        while (true) {
            g = g.finer();
            Assert.assertEquals(granularities[count--], g);
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testRemoveLocatorIteratorKeysComplains() {
        Granularity.FULL.locatorKeys(23, 0, 1000000000).iterator().remove();
    }
    
    @Test(expected = RuntimeException.class)
    public void testToBeforeFromInterval() {
        Granularity.granularityFromPointsInInterval(10000000, 0, 100);
    }
    
    @Test
    public void testGranularityEqualityAndFromString() {
        for (Granularity g : Granularity.granularities()) {
            Assert.assertTrue(g == Granularity.fromString(g.name()));
            Assert.assertTrue(g.equals(Granularity.fromString(g.name())));
            Assert.assertFalse(g.equals(new Object()));
            // throw this one in too.
            Assert.assertEquals(g.name(), g.toString());
        }
        Assert.assertNull(Granularity.fromString("nonexistant granularity"));
    }

    @Test
    public void testEquals() {
        Granularity gran1 = Granularity.MIN_5;
        Granularity gran2 = Granularity.MIN_5;
        Granularity gran3 = Granularity.MIN_1440;
        Average avg = new Average(1, 2.0);

        Assert.assertEquals(gran2, gran1);
        Assert.assertFalse(gran1.equals(gran3));
        Assert.assertFalse(gran1.equals(avg));
    }

    @Test
    public void testFromString() {
        Granularity gran;
        String s;

        s = "metrics_full";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.FULL));

        s = "metrics_5m";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_5));

        s = "metrics_20m";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_20));

        s = "metrics_60m";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_60));

        s = "metrics_240m";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_240));

        s = "metrics_1440m";
        gran = Granularity.fromString(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_1440));

        s = "metrics_1990m";
        gran = Granularity.fromString(s);
        Assert.assertNull(gran);
    }

    @Test
    public void testGranularityFromKey() {
        Granularity gran;
        String s;

        s = "metrics_full,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.FULL));

        s = "metrics_5m,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_5));

        s = "metrics_20m,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_20));

        s = "metrics_60m,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_60));

        s = "metrics_240m,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_240));

        s = "metrics_1440m,1,123";
        gran = Granularity.granularityFromKey(s);
        Assert.assertTrue(gran.equals(Granularity.MIN_1440));

        try {
            s = "metrics_1990m,1,123";
            gran = Granularity.granularityFromKey(s);
            Assert.fail("Should have failed");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("Unexpected granularity: metrics_1990m,1,123", e.getMessage());
        }
    }

    @Test
    public void testShardFromKey() {
        Granularity gran = Granularity.FULL;
        String s = gran.formatLocatorKey(1,123);
        int myInt = Granularity.shardFromKey(s);
        
        Assert.assertEquals(123, myInt);
    }

    @Test
    public void testSlotFromKey() {
        Granularity gran = Granularity.FULL;
        String s = gran.formatLocatorKey(1,123);
        int myInt = Granularity.slotFromKey(s);
        
        Assert.assertEquals(1, myInt);
    }

    @Test
    public void testBadGranularityFromPointsInterval() {
        try {
            Granularity.granularityFromPointsInInterval(2, 1, 3);
            Assert.fail("Should not have worked");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("Invalid interval specified for fromPointsInInterval", e.getMessage());
        }
    }

    @Test
    public void testLocatorKeysHasNextCurGreaterThanOrEqualToStartSlot() {
        Granularity gran = Granularity.FULL;
        Iterable<String> locatorKeys;
        Iterator<String> iter;
        String[] locatorKeysArr = new String[4032];
        int i = 0;

        //startSlot = 2, stopSlot = 1;
        locatorKeys = gran.locatorKeys(2, 600000, 0);

        iter = locatorKeys.iterator();

        //hasNext will get to case where startSlot > stopSlot
        Assert.assertTrue(iter.hasNext());
        //cur = startSlot, so iter will go from 2 to 4032 (numSlots)
        while (iter.hasNext()) {
            locatorKeysArr[i] = iter.next();
            i++;
        }

        //make sure it iterated the right number of times
        Assert.assertNotNull(locatorKeysArr[4030]);
        Assert.assertNull(locatorKeysArr[4031]);

        try {
            iter.remove();
            Assert.fail("Should not have worked");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("Not supported", e.getMessage());
        }
    }

    @Test
    public void testIsCoarser() {
        Assert.assertTrue(!Granularity.FULL.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(!Granularity.MIN_5.isCoarser(Granularity.MIN_20));
        Assert.assertTrue(!Granularity.MIN_20.isCoarser(Granularity.MIN_60));
        Assert.assertTrue(!Granularity.MIN_60.isCoarser(Granularity.MIN_240));
        Assert.assertTrue(!Granularity.MIN_240.isCoarser(Granularity.MIN_1440));

        Assert.assertTrue(Granularity.MIN_5.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_20.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_60.isCoarser(Granularity.MIN_20));
        Assert.assertTrue(Granularity.MIN_240.isCoarser(Granularity.MIN_60));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.MIN_240));
    }
}
