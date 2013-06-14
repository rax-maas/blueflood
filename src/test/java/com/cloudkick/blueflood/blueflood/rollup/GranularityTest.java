package com.cloudkick.blueflood.rollup;

import com.cloudkick.blueflood.types.Average;
import com.cloudkick.blueflood.types.Range;
import com.cloudkick.blueflood.utils.TimeValue;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GranularityTest {

    final long baseMillis = 1335820166000L;

    @Test
    public void testFromPointsInInterval() throws Exception {
        TestCase.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 86400).name());
        TestCase.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 288).name());
        TestCase.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 72).name());
        TestCase.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 24).name());
        TestCase.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 6).name());
        TestCase.assertEquals(Granularity.MIN_1440.name(), Granularity.granularityFromPointsInInterval(0, 86400000, 1).name());

        TestCase.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 800).name());
        TestCase.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 288).name()); // 144 5m points vs 1440 full points.
        TestCase.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 144).name());
        TestCase.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 35).name());
        TestCase.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 11).name());
        TestCase.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval(0, 43200000, 3).name());

        TestCase.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval(baseMillis, baseMillis + 86400000, 287).name());
    }

    @Test
    public void testRangesForInterval() throws Exception {
        TestCase.assertEquals(1, countIterable(Range.rangesForInterval(Granularity.FULL, 0, 86399000)));
        TestCase.assertEquals(288, countIterable(Range.rangesForInterval(Granularity.MIN_5, 0, 86399000)));
        TestCase.assertEquals(72, countIterable(Range.rangesForInterval(Granularity.MIN_20, 0, 86399000)));
        TestCase.assertEquals(24, countIterable(Range.rangesForInterval(Granularity.MIN_60, 0, 86399000)));
        TestCase.assertEquals(6, countIterable(Range.rangesForInterval(Granularity.MIN_240, 0, 86399000)));
        TestCase.assertEquals(1, countIterable(Range.rangesForInterval(Granularity.MIN_1440, 0, 86399000)));
        // The following test case was added after a production issue in which the call to rangesForInterval never
        // terminated.
        TestCase.assertEquals(7, countIterable(Range.rangesForInterval(Granularity.MIN_240, System.currentTimeMillis() - (24 * 60 * 60 * 1000), System.currentTimeMillis())));
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
        TestCase.assertEquals(Granularity.MIN_20, Granularity.granularityFromPointsInInterval(start, start + 10000000, desiredPoints));
        TestCase.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval(start, start + 1000000, desiredPoints));
      
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
            TestCase.assertEquals(
                    String.format("%d points", entry.getKey()),
                    entry.getValue(), 
                    actual);
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testTooCoarse() {
        Granularity g = Granularity.FULL;
        while (true)
            g = g.coarser();
    }
    
    @Test(expected = RuntimeException.class)
    public void testTooFine() {
        Granularity g = Granularity.MIN_1440;
        while (true)
            g = g.finer();
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
        for (Granularity g : Granularity.values()) {
            TestCase.assertTrue(g == Granularity.fromString(g.name()));
            TestCase.assertTrue(g.equals(Granularity.fromString(g.name())));
            TestCase.assertFalse(g.equals(new Object()));
            // throw this one in too.
            TestCase.assertEquals(g.name(), g.toString());
        }
        TestCase.assertNull(Granularity.fromString("nonexistant granularity"));
    }

    @Test
    public void testEquals() {
        Granularity gran1 = Granularity.MIN_5;
        Granularity gran2 = Granularity.MIN_5;
        Granularity gran3 = Granularity.MIN_1440;
        Average avg = new Average(1, 2.0);

        TestCase.assertEquals(gran2, gran1);
        TestCase.assertFalse(gran1.equals(gran3));
        TestCase.assertFalse(gran1.equals(avg));
    }

    @Test
    public void testFromString() {
        Granularity gran;
        String s;

        s = "metrics_full";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.FULL));

        s = "metrics_5m";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_5));

        s = "metrics_20m";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_20));

        s = "metrics_60m";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_60));

        s = "metrics_240m";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_240));

        s = "metrics_1440m";
        gran = Granularity.fromString(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_1440));

        s = "metrics_1990m";
        gran = Granularity.fromString(s);
        TestCase.assertNull(gran);
    }

    @Test
    public void testGranularityFromKey() {
        Granularity gran;
        String s;

        s = "metrics_full,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.FULL));

        s = "metrics_5m,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_5));

        s = "metrics_20m,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_20));

        s = "metrics_60m,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_60));

        s = "metrics_240m,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_240));

        s = "metrics_1440m,1,123";
        gran = Granularity.granularityFromKey(s);
        TestCase.assertTrue(gran.equals(Granularity.MIN_1440));

        try {
            s = "metrics_1990m,1,123";
            gran = Granularity.granularityFromKey(s);
            TestCase.fail("Should have failed");
        }
        catch (RuntimeException e) {
            TestCase.assertEquals("Unexpected granularity: metrics_1990m,1,123", e.getMessage());
        }
    }

    @Test
    public void testShardFromKey() {
        Granularity gran = Granularity.FULL;
        String s = gran.formatLocatorKey(1,123);
        int myInt = Granularity.shardFromKey(s);
        
        TestCase.assertEquals(123, myInt);
    }

    @Test
    public void testSlotFromKey() {
        Granularity gran = Granularity.FULL;
        String s = gran.formatLocatorKey(1,123);
        int myInt = Granularity.slotFromKey(s);
        
        TestCase.assertEquals(1, myInt);
    }

    @Test
    public void testGetTTL() {
        Granularity gran = Granularity.FULL;

        TimeValue tv = gran.getTTL();
        TimeValue tv2 = new TimeValue(1, TimeUnit.DAYS);
        
        TestCase.assertEquals(tv2.toDays(), tv.toDays());
    }

    @Test
    public void testBadGranularityFromPointsInterval() {
        try {
            Granularity.granularityFromPointsInInterval(2, 1, 3);
            TestCase.fail("Should not have worked");
        }
        catch (RuntimeException e) {
            TestCase.assertEquals("Invalid interval specified for fromPointsInInterval", e.getMessage());
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
        TestCase.assertTrue(iter.hasNext());
        //cur = startSlot, so iter will go from 2 to 4032 (numSlots)
        while (iter.hasNext()) {
            locatorKeysArr[i] = iter.next();
            i++;
        }

        //make sure it iterated the right number of times
        TestCase.assertNotNull(locatorKeysArr[4030]);
        TestCase.assertNull(locatorKeysArr[4031]);

        try {
            iter.remove();
            TestCase.fail("Should not have worked");
        }
        catch (RuntimeException e) {
            TestCase.assertEquals("Not supported", e.getMessage());
        }
    }
}
