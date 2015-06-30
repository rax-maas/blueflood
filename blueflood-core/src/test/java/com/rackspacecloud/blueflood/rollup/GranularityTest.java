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
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class GranularityTest {

    final long fromBaseMillis = Calendar.getInstance().getTimeInMillis();
    final long toBaseMillis = fromBaseMillis+ 604800000;
    final long milliSecondsInADay = 86400 * 1000;

    //An old timestamp that signifies ttld out data.
    // 619200000 is 8 days, just 1 day more than 7 days ie ttl limit of full gran
    final long oldFromBaseMillis_FullGran = fromBaseMillis - (8 * milliSecondsInADay);
    // 15 days old from toTimeStamp.
    final long oldToBaseMillis_FullGran = oldFromBaseMillis_FullGran + (7 * milliSecondsInADay);

    // A 16 days old time stamp beyond the ttl of 5m granularity
    final long oldFromBaseMillis_5m = fromBaseMillis - (16 * milliSecondsInADay);
    final long oldToBaseMillis_5m = oldFromBaseMillis_5m + (7 * milliSecondsInADay);

    // A 30 days old time stamp beyond the ttl of 20m granularity
    final long oldFromBaseMillis_20m = fromBaseMillis - (30 * milliSecondsInADay);
    final long oldToBaseMillis_20m = oldFromBaseMillis_20m + (7 * milliSecondsInADay);

    // A 160 days old time stamp beyond the ttl of 60m granularity
    final long oldFromBaseMillis_60m = fromBaseMillis - (160 * milliSecondsInADay);
    final long oldToBaseMillis_60m = oldFromBaseMillis_60m + (7 * milliSecondsInADay);

    //A 400 day old time stamp beyond the ttl of 240m granularity
    final long oldFromBaseMillis_240m = fromBaseMillis - (400 * milliSecondsInADay);
    final long oldToBaseMillis_240m = oldFromBaseMillis_240m + (7 * milliSecondsInADay);


    @Test
    public void testFromPointsInInterval_1WeekInterval_OldAndNew() throws Exception {
        Assert.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 86400).name());

        // The timestamp is too old for full data, so it goes to the next granularity ie 5m
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",oldFromBaseMillis_FullGran, oldToBaseMillis_FullGran, 86400).name());

        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 1152).name());

        //The timestamp is too old for 5m. So it goes to the next granularity ie 20m
        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",oldFromBaseMillis_5m, oldToBaseMillis_5m, 1152).name());


        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 576).name());

        //The timestamp is too old for 20m. So it goes to the next granularity ie 60m
        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",oldFromBaseMillis_20m, oldToBaseMillis_20m, 576).name());


        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 96).name());

        //The timestamp is too old for 60m. So it goes to the next granularity ie 240m
        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",oldFromBaseMillis_60m, oldToBaseMillis_60m, 96).name());


        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 24).name());

        //The timestamp is too old for 240m. So it goes to the next granularity ie 1440m
        Assert.assertEquals(Granularity.MIN_1440.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",oldFromBaseMillis_240m, oldToBaseMillis_240m, 24).name());


        Assert.assertEquals(Granularity.MIN_1440.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, toBaseMillis, 1).name());
    }

    @Test
    public void testFromPointsInterval_ADayInterval() throws Exception {
        Assert.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 86400).name());
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 288).name());
        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 72).name());
        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 24).name());
        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 6).name());
        Assert.assertEquals(Granularity.MIN_1440.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+86400000, 1).name());
    }

    @Test
    public void testFromPointsInInterval_LessThanADayInterval() throws Exception {
        Assert.assertEquals(Granularity.FULL.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 800).name());
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 288).name()); // 144 5m points vs 1440 full points.
        Assert.assertEquals(Granularity.MIN_5.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 144).name());
        Assert.assertEquals(Granularity.MIN_20.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 35).name());
        Assert.assertEquals(Granularity.MIN_60.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 11).name());
        Assert.assertEquals(Granularity.MIN_240.name(), Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+43200000, 3).name());
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
        long start = Calendar.getInstance().getTimeInMillis();
        // 10000000ms == 166.67 min.  166/20 is 8 points. 166/5 is 33 points.  The old algorithm returned the latter, which is too many.
        // 1000000ms == 16.67 min. 16/20 is 0, 16/5 is 3 points, 16/full = 32 points.
        // is too many.
        Assert.assertEquals(Granularity.MIN_20, Granularity.granularityFromPointsInInterval("TENANTID1234",start, start + 10000000, desiredPoints));
        Assert.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval("TENANTID1234",start, start + 1000000, desiredPoints));
      
        // Test edge cases using a 100000000 millisecond swath. For reference 100k secs generates:
        // 3333.33 full res points (at 30s per check)
        // 333.33 5min points
        // 83.33 20min points
        // 27.78 60min points
        // 6.94 240min points
        // 1.15 1440min points
        // To compute the boundaries used below, solve the parallel equation for x (I suggest Wolfram Alpha):
        //    1/a * x = higher_res_point_count
        //      a * x = lower_res_point_count
        Map<Integer, Granularity> expectedGranularities = new HashMap<Integer, Granularity>() {{
            // Request sub 30 second periods
            put(5000, Granularity.FULL);

            // Edge between FULL and MIN_5 (boundary is ~1054.09 points)
            put(1055, Granularity.FULL);
            put(1054, Granularity.MIN_5);

            // Edge between MIN_5 and MIN_20 (boundary is ~166.66 points)
            put(167, Granularity.MIN_5);
            put(166, Granularity.MIN_20);

            // Edge between MIN_20 and MIN_60 (boundary is ~48.11 points)
            put(49, Granularity.MIN_20);
            put(48, Granularity.MIN_60);

            // Edge between MIN_60 and MIN_240 (boundary is ~13.89 points)
            put(14, Granularity.MIN_60);
            put(13, Granularity.MIN_240);

            // Edge between MIN_240 and MIN_1440 (boundary is ~2.83 points)
            put(3, Granularity.MIN_240);
            put(2, Granularity.MIN_1440);

            put(1, Granularity.MIN_1440); // Request > 1 day periods
        }};
        
        for (Map.Entry<Integer, Granularity> entry : expectedGranularities.entrySet()) {
            Granularity actual = Granularity.granularityFromPointsInInterval("TENANTID1234",start, start+100000000, entry.getKey());
            Assert.assertEquals(
                    String.format("%d points", entry.getKey()),
                    entry.getValue(), 
                    actual);
        }
    }

    @Test
    public void testCommonPointRequests() {
        long HOUR = 3600000;
        long DAY = 24 * HOUR;

        // 300 points covering 1 hour -> FULL (120 points)
        Assert.assertEquals(Granularity.FULL, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+HOUR, 300));

        // 300 points covering 8 hours -> MIN_5 (96 points - 8 hours is actually a really unfortunate interval)
        Assert.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+(8 * HOUR), 300));

        // 300 points covering 12 hours -> MIN_5 (144 points)
        Assert.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+(12 * HOUR), 300));

        // 300 points covering 1 day -> MIN_5 (288 points)
        Assert.assertEquals(Granularity.MIN_5, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+DAY, 300));

        // 300 points covering 1 week -> MIN_20 (504 points)
        Assert.assertEquals(Granularity.MIN_20, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+(7 * DAY), 300));

        // 300 points covering 1 month -> MIN_240 (180 points)
        Assert.assertEquals(Granularity.MIN_240, Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis, fromBaseMillis+(30 * DAY), 300));
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
    public void testToBeforeFromInterval() {
        Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis+10000000, fromBaseMillis+0, 100);
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
    public void testBadGranularityFromPointsInterval() {
        try {
            Granularity.granularityFromPointsInInterval("TENANTID1234",fromBaseMillis+2, fromBaseMillis+1, 3);
            Assert.fail("Should not have worked");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("Invalid interval specified for fromPointsInInterval", e.getMessage());
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
