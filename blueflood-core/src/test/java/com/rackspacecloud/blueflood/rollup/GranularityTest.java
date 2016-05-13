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
import com.rackspacecloud.blueflood.utils.Clock;
import org.joda.time.Instant;
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

    @Test
    public void coarserReturnsCoarser() throws GranularityException {
        Assert.assertSame(Granularity.MIN_5, Granularity.FULL.coarser());
        Assert.assertSame(Granularity.MIN_20, Granularity.MIN_5.coarser());
        Assert.assertSame(Granularity.MIN_60, Granularity.MIN_20.coarser());
        Assert.assertSame(Granularity.MIN_240, Granularity.MIN_60.coarser());
        Assert.assertSame(Granularity.MIN_1440, Granularity.MIN_240.coarser());
    }

    @Test(expected = GranularityException.class)
    public void testTooCoarse() throws Exception {
        // when
        Granularity.MIN_1440.coarser();
        // then
        // the exception is thrown
    }

    @Test
    public void finerReturnsFiner() throws GranularityException {
        Assert.assertSame(Granularity.FULL, Granularity.MIN_5.finer());
        Assert.assertSame(Granularity.MIN_5, Granularity.MIN_20.finer());
        Assert.assertSame(Granularity.MIN_20, Granularity.MIN_60.finer());
        Assert.assertSame(Granularity.MIN_60, Granularity.MIN_240.finer());
        Assert.assertSame(Granularity.MIN_240, Granularity.MIN_1440.finer());
    }

    @Test(expected = GranularityException.class)
    public void testTooFine() throws Exception {
        // when
        Granularity.FULL.finer();
        // then
        // the exception is thrown
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
    public void equalsWithSameValueReturnsTrue() {
        Assert.assertTrue(Granularity.FULL.equals(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_5.equals(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_20.equals(Granularity.MIN_20));
        Assert.assertTrue(Granularity.MIN_60.equals(Granularity.MIN_60));
        Assert.assertTrue(Granularity.MIN_240.equals(Granularity.MIN_240));
        Assert.assertTrue(Granularity.MIN_1440.equals(Granularity.MIN_1440));
    }

    @Test
    public void equalsWithDifferentValueReturnsFalse() {
        Assert.assertFalse(Granularity.FULL.equals(Granularity.MIN_5));
        Assert.assertFalse(Granularity.FULL.equals(Granularity.MIN_20));
        Assert.assertFalse(Granularity.FULL.equals(Granularity.MIN_60));
        Assert.assertFalse(Granularity.FULL.equals(Granularity.MIN_240));
        Assert.assertFalse(Granularity.FULL.equals(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_5.equals(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_5.equals(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_5.equals(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_5.equals(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_5.equals(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_20.equals(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_20.equals(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_20.equals(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_20.equals(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_20.equals(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_60.equals(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_60.equals(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_60.equals(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_60.equals(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_60.equals(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_240.equals(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_240.equals(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_240.equals(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_240.equals(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_240.equals(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_1440.equals(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_1440.equals(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_1440.equals(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_1440.equals(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_1440.equals(Granularity.MIN_240));
    }

    @Test
    public void equalsWithNullReturnsFalse() {
        Assert.assertFalse(Granularity.FULL.equals(null));
        Assert.assertFalse(Granularity.MIN_5.equals(null));
        Assert.assertFalse(Granularity.MIN_20.equals(null));
        Assert.assertFalse(Granularity.MIN_60.equals(null));
        Assert.assertFalse(Granularity.MIN_240.equals(null));
        Assert.assertFalse(Granularity.MIN_1440.equals(null));
    }

    @Test
    public void equalsWithNonGranularityObjectReturnsFalse() {
        Assert.assertFalse(Granularity.FULL.equals(new Object()));
        Assert.assertFalse(Granularity.MIN_5.equals(new Object()));
        Assert.assertFalse(Granularity.MIN_20.equals(new Object()));
        Assert.assertFalse(Granularity.MIN_60.equals(new Object()));
        Assert.assertFalse(Granularity.MIN_240.equals(new Object()));
        Assert.assertFalse(Granularity.MIN_1440.equals(new Object()));
    }

    @Test
    public void fromStringFull() {
        // given
        String s = "metrics_full";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.FULL, gran);
    }

    @Test
    public void fromString5m() {
        // given
        String s = "metrics_5m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_5, gran);
    }

    @Test
    public void fromString20m() {
        // given
        String s = "metrics_20m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void fromString60m() {
        // given
        String s = "metrics_60m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_60, gran);
    }

    @Test
    public void fromString240m() {
        // given
        String s = "metrics_240m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertSame(Granularity.MIN_240, gran);
    }

    @Test
    public void fromString1440m() {
        // given
        String s = "metrics_1440m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test
    public void fromStringOtherReturnsNull() {
        // given
        String s = "metrics_1990m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNull(gran);
    }

    @Test
    public void fromStringFullShortName() {
        // given
        String s = "full";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.FULL, gran);
    }

    @Test
    public void fromString5mShortName() {
        // given
        String s = "5m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_5, gran);
    }

    @Test
    public void fromString20mShortName() {
        // given
        String s = "20m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void fromString60mShortName() {
        // given
        String s = "60m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_60, gran);
    }

    @Test
    public void fromString240mShortName() {
        // given
        String s = "240m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertSame(Granularity.MIN_240, gran);
    }

    @Test
    public void fromString1440mShortName() {
        // given
        String s = "1440m";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNotNull(gran);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test
    public void fromStringCaseSensitive() {
        // given
        String s = "METRICS_1440M";
        // when
        Granularity gran = Granularity.fromString(s);
        // then
        Assert.assertNull(gran);
    }

    @Test
    public void fromStringNullReturnsNull() {
        // when
        Granularity gran = Granularity.fromString(null);
        // then
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
    public void coarserGranularitiesAreCoarserThanFinerOnes() {
        // NOTE: This would be a good candidate for

        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_5.isCoarser(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_20.isCoarser(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_60.isCoarser(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_240.isCoarser(Granularity.MIN_1440));

        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.MIN_20));
        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.MIN_60));
        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.MIN_240));
        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_5.isCoarser(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_5.isCoarser(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_5.isCoarser(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_20.isCoarser(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_20.isCoarser(Granularity.MIN_1440));
        Assert.assertFalse(Granularity.MIN_60.isCoarser(Granularity.MIN_1440));

        Assert.assertTrue(Granularity.MIN_5.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_20.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_60.isCoarser(Granularity.MIN_20));
        Assert.assertTrue(Granularity.MIN_240.isCoarser(Granularity.MIN_60));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.MIN_240));

        Assert.assertTrue(Granularity.MIN_20.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_60.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_60.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_240.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_240.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_240.isCoarser(Granularity.MIN_20));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.FULL));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.MIN_5));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.MIN_20));
        Assert.assertTrue(Granularity.MIN_1440.isCoarser(Granularity.MIN_60));

        Assert.assertFalse(Granularity.FULL.isCoarser(Granularity.FULL));
        Assert.assertFalse(Granularity.MIN_5.isCoarser(Granularity.MIN_5));
        Assert.assertFalse(Granularity.MIN_20.isCoarser(Granularity.MIN_20));
        Assert.assertFalse(Granularity.MIN_60.isCoarser(Granularity.MIN_60));
        Assert.assertFalse(Granularity.MIN_240.isCoarser(Granularity.MIN_240));
        Assert.assertFalse(Granularity.MIN_1440.isCoarser(Granularity.MIN_1440));
    }

    @Test(expected = NullPointerException.class)
    public void isCoarserWithNullThrowsException() {
        Assert.assertFalse(Granularity.FULL.isCoarser(null));
    }

    @Test
    public void snapMillisOnFullReturnsSameValue() {
        Assert.assertEquals(1234L, Granularity.FULL.snapMillis(1234L));
        Assert.assertEquals(1000L, Granularity.FULL.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.FULL.snapMillis(0L));
        Assert.assertEquals(1234567L, Granularity.FULL.snapMillis(1234567L));
    }

    @Test
    public void snapMillisOnOtherReturnsSnappedValue() {
        Assert.assertEquals(0L, Granularity.MIN_5.snapMillis(1234L));
        Assert.assertEquals(0L, Granularity.MIN_5.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.MIN_5.snapMillis(0L));
        Assert.assertEquals(0L, Granularity.MIN_5.snapMillis(299999L));
        Assert.assertEquals(300000L, Granularity.MIN_5.snapMillis(300000L));
        Assert.assertEquals(300000L, Granularity.MIN_5.snapMillis(300001L));
        Assert.assertEquals(1200000L, Granularity.MIN_5.snapMillis(1234567L));

        Assert.assertEquals(0L, Granularity.MIN_20.snapMillis(1234L));
        Assert.assertEquals(0L, Granularity.MIN_20.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.MIN_20.snapMillis(0L));
        Assert.assertEquals(0L, Granularity.MIN_20.snapMillis(1199999L));
        Assert.assertEquals(1200000L, Granularity.MIN_20.snapMillis(1200000));
        Assert.assertEquals(1200000L, Granularity.MIN_20.snapMillis(1200001L));
        Assert.assertEquals(1200000L, Granularity.MIN_20.snapMillis(1234567L));

        Assert.assertEquals(0L, Granularity.MIN_60.snapMillis(1234L));
        Assert.assertEquals(0L, Granularity.MIN_60.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.MIN_60.snapMillis(0L));
        Assert.assertEquals(0L, Granularity.MIN_60.snapMillis(3599999L));
        Assert.assertEquals(3600000L, Granularity.MIN_60.snapMillis(3600000L));
        Assert.assertEquals(3600000L, Granularity.MIN_60.snapMillis(3600001L));
        Assert.assertEquals(122400000L, Granularity.MIN_60.snapMillis(123456789L));

        Assert.assertEquals(0L, Granularity.MIN_240.snapMillis(1234L));
        Assert.assertEquals(0L, Granularity.MIN_240.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.MIN_240.snapMillis(0L));
        Assert.assertEquals(0L, Granularity.MIN_240.snapMillis(14399999L));
        Assert.assertEquals(14400000L, Granularity.MIN_240.snapMillis(14400000L));
        Assert.assertEquals(14400000L, Granularity.MIN_240.snapMillis(14400001L));
        Assert.assertEquals(115200000L, Granularity.MIN_240.snapMillis(123456789L));

        Assert.assertEquals(0L, Granularity.MIN_1440.snapMillis(1234L));
        Assert.assertEquals(0L, Granularity.MIN_1440.snapMillis(1000L));
        Assert.assertEquals(0L, Granularity.MIN_1440.snapMillis(0L));
        Assert.assertEquals(0L, Granularity.MIN_1440.snapMillis(86399999L));
        Assert.assertEquals(86400000L, Granularity.MIN_1440.snapMillis(86400000L));
        Assert.assertEquals(86400000L, Granularity.MIN_1440.snapMillis(86400001L));
        Assert.assertEquals(86400000L, Granularity.MIN_1440.snapMillis(123456789L));
    }

    @Test
    public void millisToSlotReturnsNumberOfSlotForGivenTime() {
        Assert.assertEquals(0, Granularity.millisToSlot(0));
        Assert.assertEquals(0, Granularity.millisToSlot(1));
        Assert.assertEquals(0, Granularity.millisToSlot(299999L));
        Assert.assertEquals(1, Granularity.millisToSlot(300000L));
        Assert.assertEquals(1, Granularity.millisToSlot(300001L));
        Assert.assertEquals(1, Granularity.millisToSlot(599999L));
        Assert.assertEquals(2, Granularity.millisToSlot(600000L));
        Assert.assertEquals(2, Granularity.millisToSlot(600001L));
        Assert.assertEquals(4030, Granularity.millisToSlot(1209299999L));
        Assert.assertEquals(4031, Granularity.millisToSlot(1209300000L));
        Assert.assertEquals(4031, Granularity.millisToSlot(1209300001L));
        Assert.assertEquals(4031, Granularity.millisToSlot(1209599999L));
        Assert.assertEquals(0, Granularity.millisToSlot(1209600000L));
    }

    @Test
    public void millisToSlotWithNegativeReturnsNegative() {
        Assert.assertEquals(0, Granularity.millisToSlot(-0));
        Assert.assertEquals(0, Granularity.millisToSlot(-1));
        Assert.assertEquals(0, Granularity.millisToSlot(-299999L));
        Assert.assertEquals(-1, Granularity.millisToSlot(-300000L));
        Assert.assertEquals(-1, Granularity.millisToSlot(-300001L));
        Assert.assertEquals(-1, Granularity.millisToSlot(-599999L));
        Assert.assertEquals(-2, Granularity.millisToSlot(-600000L));
        Assert.assertEquals(-2, Granularity.millisToSlot(-600001L));
        Assert.assertEquals(-4030, Granularity.millisToSlot(-1209299999L));
        Assert.assertEquals(-4031, Granularity.millisToSlot(-1209300000L));
        Assert.assertEquals(-4031, Granularity.millisToSlot(-1209300001L));
        Assert.assertEquals(-4031, Granularity.millisToSlot(-1209599999L));
        Assert.assertEquals(0, Granularity.millisToSlot(-1209600000L));
    }

    @Test
    public void slotReturnsTheSlotNumber() {

        Assert.assertEquals(0, Granularity.FULL.slot(1234L));
        Assert.assertEquals(0, Granularity.FULL.slot(1000L));
        Assert.assertEquals(0, Granularity.FULL.slot(0L));
        Assert.assertEquals(0, Granularity.FULL.slot(299999L));
        Assert.assertEquals(1, Granularity.FULL.slot(300000L));
        Assert.assertEquals(1, Granularity.FULL.slot(300001L));
        Assert.assertEquals(4, Granularity.FULL.slot(1234567L));
        Assert.assertEquals(4031, Granularity.FULL.slot(1209599999L));
        Assert.assertEquals(0, Granularity.FULL.slot(1209600000L));
        Assert.assertEquals(0, Granularity.FULL.slot(1209600001L));

        Assert.assertEquals(0, Granularity.MIN_5.slot(1234L));
        Assert.assertEquals(0, Granularity.MIN_5.slot(1000L));
        Assert.assertEquals(0, Granularity.MIN_5.slot(0L));
        Assert.assertEquals(0, Granularity.MIN_5.slot(299999L));
        Assert.assertEquals(1, Granularity.MIN_5.slot(300000L));
        Assert.assertEquals(1, Granularity.MIN_5.slot(300001L));
        Assert.assertEquals(4, Granularity.MIN_5.slot(1234567L));
        Assert.assertEquals(4031, Granularity.MIN_5.slot(1209599999L));
        Assert.assertEquals(0, Granularity.MIN_5.slot(1209600000L));
        Assert.assertEquals(0, Granularity.MIN_5.slot(1209600001L));

        Assert.assertEquals(0, Granularity.MIN_20.slot(1234L));
        Assert.assertEquals(0, Granularity.MIN_20.slot(1000L));
        Assert.assertEquals(0, Granularity.MIN_20.slot(0L));
        Assert.assertEquals(0, Granularity.MIN_20.slot(1199999L));
        Assert.assertEquals(1, Granularity.MIN_20.slot(1200000));
        Assert.assertEquals(1, Granularity.MIN_20.slot(1200001L));
        Assert.assertEquals(1, Granularity.MIN_20.slot(1234567L));
        Assert.assertEquals(102, Granularity.MIN_20.slot(123456789L));
        Assert.assertEquals(1007, Granularity.MIN_20.slot(1209599999L));
        Assert.assertEquals(0, Granularity.MIN_20.slot(1209600000L));
        Assert.assertEquals(0, Granularity.MIN_20.slot(1209600001L));

        Assert.assertEquals(0, Granularity.MIN_60.slot(1234L));
        Assert.assertEquals(0, Granularity.MIN_60.slot(1000L));
        Assert.assertEquals(0, Granularity.MIN_60.slot(0L));
        Assert.assertEquals(0, Granularity.MIN_60.slot(3599999L));
        Assert.assertEquals(1, Granularity.MIN_60.slot(3600000L));
        Assert.assertEquals(1, Granularity.MIN_60.slot(3600001L));
        Assert.assertEquals(34, Granularity.MIN_60.slot(123456789L));
        Assert.assertEquals(69, Granularity.MIN_60.slot(12345678901L));
        Assert.assertEquals(335, Granularity.MIN_60.slot(1209599999L));
        Assert.assertEquals(0, Granularity.MIN_60.slot(1209600000L));
        Assert.assertEquals(0, Granularity.MIN_60.slot(1209600001L));

        Assert.assertEquals(0, Granularity.MIN_240.slot(1234L));
        Assert.assertEquals(0, Granularity.MIN_240.slot(1000L));
        Assert.assertEquals(0, Granularity.MIN_240.slot(0L));
        Assert.assertEquals(0, Granularity.MIN_240.slot(14399999L));
        Assert.assertEquals(1, Granularity.MIN_240.slot(14400000L));
        Assert.assertEquals(1, Granularity.MIN_240.slot(14400001L));
        Assert.assertEquals(8, Granularity.MIN_240.slot(123456789L));
        Assert.assertEquals(17, Granularity.MIN_240.slot(12345678901L));
        Assert.assertEquals(83, Granularity.MIN_240.slot(1209599999L));
        Assert.assertEquals(0, Granularity.MIN_240.slot(1209600000L));
        Assert.assertEquals(0, Granularity.MIN_240.slot(1209600001L));

        Assert.assertEquals(0, Granularity.MIN_1440.slot(1234L));
        Assert.assertEquals(0, Granularity.MIN_1440.slot(1000L));
        Assert.assertEquals(0, Granularity.MIN_1440.slot(0L));
        Assert.assertEquals(0, Granularity.MIN_1440.slot(86399999L));
        Assert.assertEquals(1, Granularity.MIN_1440.slot(86400000L));
        Assert.assertEquals(1, Granularity.MIN_1440.slot(86400001L));
        Assert.assertEquals(2, Granularity.MIN_1440.slot(12345678901L));
        Assert.assertEquals(13, Granularity.MIN_1440.slot(1209599999L));
        Assert.assertEquals(0, Granularity.MIN_1440.slot(1209600000L));
        Assert.assertEquals(0, Granularity.MIN_1440.slot(1209600001L));
    }

    @Test
    public void granFromPointsLinear100PointsBoundaryBetween5And20() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 48000000, 100, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 48000001, 100, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void granFromPointsLinear1000PointsBoundaryBetween5And20() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 480000000L, 1000, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 480000001L, 1000, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void granFromPointsLinear10000PointsBoundaryBetween5And20() {

        // TODO: Change the the granularityFromPointsLinear method to use long
        // instead of int. The method returns nulls in this case because of
        // overflow.

        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 4800000000L, 10000, "LINEAR", 1);
        Assert.assertNull(gran); // Should be Granularity.MIN_5
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 4800000001L, 10000, "LINEAR", 1);
        Assert.assertNull(gran); // Should be Granularity.MIN_20
    }

    @Test
    public void granFromPointsLinearBoundaryBetween20And60() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 18000000, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 18000001, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_60, gran);
    }

    @Test
    public void granFromPointsLinearBoundaryBetween60And240() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 57600000, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_60, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 57600001, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_240, gran);
    }

    @Test
    public void granFromPointsLinearBoundaryBetween240And1440() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 246857142, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_240, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 246857143, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_240, gran); // Should be Granularity.MIN_1440

        // TODO: According to the math of what the granularityFromPointsLinear
        // method is doing, the changeover point for 240-to-1440 should be
        // 246857143, but the algorithm makes mistakes due to the difference in
        // precision between ints/longs and doubles. With the error, the
        // changeover point becomes 259200000.
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 259199999, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_240, gran); // Should be Granularity.MIN_1440
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 259200000, 10, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test(expected = RuntimeException.class)
    public void granFromPointsWithFromGreaterThanToThrowsException() {
        // when
        Granularity gran = Granularity.granularityFromPointsInInterval("abc123", 10, 5, 10, "LINEAR", 1);
        // then
        // the exception is thrown
    }

    @Test(expected = RuntimeException.class)
    public void granFromPointsWithFromEqualToThrowsException() {
        // when
        Granularity gran = Granularity.granularityFromPointsInInterval("abc123", 10, 10, 10, "LINEAR", 1);
        // then
        // the exception is thrown
    }

    @Test
    public void granFromPointsLessThanEqual100PointsBoundaryBetween5And20() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 30000000, 100, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 30000001, 100, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void granFromPointsLessThanEqual1000PointsBoundaryBetween5And20() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 300000000L, 1000, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 300000001L, 1000, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test(expected = NullPointerException.class)
    public void granFromPointsLessThanEqual10000PointsBoundaryBetween5And20() {

        Granularity gran;
        // when
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 3000000000L, 10000, "LESSTHANEQUAL", 1);
        // then
        // the exception is thrown

        // TODO: The method is throwing because of overflow in
        // granularityFromPointsLinear. Once that method is changed to use
        // longs, the following assertions should pass:
        //Assert.assertSame(Granularity.MIN_5, gran);
        //gran = Granularity.granularityFromPointsInInterval("abc123", 0, 3000000001L, 10000, "LESSTHANEQUAL", 1);
        //Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void granFromPointsLessThanEqualBoundaryBetween20And60() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 12000000, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 12000001, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_60, gran);
    }

    @Test
    public void granFromPointsLessThanEqualBoundaryBetween60And240() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 36000000, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_60, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 36000001, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_240, gran);
    }

    @Test
    public void granFromPointsLessThanEqualBoundaryBetween240And1440() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 144000000, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_240, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 144000001, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test
    public void granFromPointsLessThanEqualWouldBeUpperBoundaryOf1440() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 864000000, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_1440, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 864000001, 10, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }
    
    Clock alwaysZeroClock = new Clock() {
        @Override
        public Instant now() {
            return new Instant(0);
        }
    };

    @Test
    public void granFromPointsGeometricBoundaryBetween5And20() {
        Granularity gran;

        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 6000000, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 6000001, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);

        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000000, 100, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000001, 100, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);

        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 600000000, 1000, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 600000001, 1000, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void granFromPointsGeometricBoundaryBetween20And60() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 20784609, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 20784610, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_60, gran);
    }

    @Test
    public void granFromPointsGeometricBoundaryBetween60And240() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 72000000, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_60, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 72000001, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_240, gran);
    }

    @Test
    public void granFromPointsGeometricBoundaryBetween240And1440() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 352726522, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_240, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 352726523, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test
    public void granFromPointsGeometricWithDefaultClockAndAllGranularitiesSkippedReturnsCoarsest() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 6000000, 10, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 6000000, 10, "GEOMETRIC", 1);
        Assert.assertSame(Granularity.MIN_1440, gran);
    }

    @Test
    public void granFromPointsInvalidAlgorithmDefaultsToGeometric() {
        Granularity gran;
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 48000000, 100, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 48000001, 100, "LINEAR", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 30000000, 100, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 30000001, 100, "LESSTHANEQUAL", 1);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000000, 100, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000001, 100, "GEOMETRIC", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000000, 100, "INVALID", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_5, gran);
        gran = Granularity.granularityFromPointsInInterval("abc123", 0, 60000001, 100, "INVALID", 1, alwaysZeroClock);
        Assert.assertSame(Granularity.MIN_20, gran);
    }

    @Test
    public void deriveRangeFiveMinuteFirstSlot() {
        // given
        Granularity gran = Granularity.MIN_5;
        Range range;

        // when
        range = gran.deriveRange(0, 0);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(299999, range.getStop());

        // when
        range = gran.deriveRange(0, 1);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(299999, range.getStop());

        // when
        range = gran.deriveRange(0, 299999);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(299999, range.getStop());

        // when
        range = gran.deriveRange(0, 300000);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(299999, range.getStop());

        // when
        range = gran.deriveRange(0, 300001);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(299999, range.getStop());
    }

    @Test
    public void deriveRangeFiveMinuteSecondSlot() {
        // given
        Granularity gran = Granularity.MIN_5;
        Range range;

        // when
        range = gran.deriveRange(1, 0);

        // then
        Assert.assertEquals(-1209300000, range.getStart());
        Assert.assertEquals(-1209000001, range.getStop());

        // when
        range = gran.deriveRange(1, 1);

        // then
        Assert.assertEquals(-1209300000, range.getStart());
        Assert.assertEquals(-1209000001, range.getStop());

        // when
        range = gran.deriveRange(1, 299999);

        // then
        Assert.assertEquals(-1209300000, range.getStart());
        Assert.assertEquals(-1209000001, range.getStop());

        // when
        range = gran.deriveRange(1, 300000);

        // then
        Assert.assertEquals(300000, range.getStart());
        Assert.assertEquals(599999, range.getStop());

        // when
        range = gran.deriveRange(1, 300001);

        // then
        Assert.assertEquals(300000, range.getStart());
        Assert.assertEquals(599999, range.getStop());

        // when
        range = gran.deriveRange(1, 599999);

        // then
        Assert.assertEquals(300000, range.getStart());
        Assert.assertEquals(599999, range.getStop());

        // when
        range = gran.deriveRange(1, 600000);

        // then
        Assert.assertEquals(300000, range.getStart());
        Assert.assertEquals(599999, range.getStop());

        // when
        range = gran.deriveRange(1, 600001);

        // then
        Assert.assertEquals(300000, range.getStart());
        Assert.assertEquals(599999, range.getStop());
    }

    @Test
    public void deriveRangeFiveMinuteLastSlot() {
        // given
        Granularity gran = Granularity.MIN_5;
        Range range;

        // when
        range = gran.deriveRange(3, 0);

        // then
        Assert.assertEquals(-1208700000, range.getStart());
        Assert.assertEquals(-1208400001, range.getStop());

        // when
        range = gran.deriveRange(3, 1);

        // then
        Assert.assertEquals(-1208700000, range.getStart());
        Assert.assertEquals(-1208400001, range.getStop());

        // when
        range = gran.deriveRange(3, 899999);

        // then
        Assert.assertEquals(-1208700000, range.getStart());
        Assert.assertEquals(-1208400001, range.getStop());

        // when
        range = gran.deriveRange(3, 900000);

        // then
        Assert.assertEquals(900000, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(3, 900001);

        // then
        Assert.assertEquals(900000, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(3, 1199999);

        // then
        Assert.assertEquals(900000, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(3, 1200000);

        // then
        Assert.assertEquals(900000, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(3, 1200001);

        // then
        Assert.assertEquals(900000, range.getStart());
        Assert.assertEquals(1199999, range.getStop());
    }

    @Test
    public void deriveRangeTwentyMinuteFirstSlot() {
        // given
        Granularity gran = Granularity.MIN_20;
        Range range;

        // when
        range = gran.deriveRange(0, 0);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(0, 1);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(0, 1199999);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(0, 1200000);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(1199999, range.getStop());

        // when
        range = gran.deriveRange(0, 1200001);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(1199999, range.getStop());
    }

    @Test
    public void deriveRangeTwentyMinuteSecondSlot() {
        // given
        Granularity gran = Granularity.MIN_20;
        Range range;

        // when
        range = gran.deriveRange(1, 0);

        // then
        Assert.assertEquals(-1208400000, range.getStart());
        Assert.assertEquals(-1207200001, range.getStop());

        // when
        range = gran.deriveRange(1, 1);

        // then
        Assert.assertEquals(-1208400000, range.getStart());
        Assert.assertEquals(-1207200001, range.getStop());

        // when
        range = gran.deriveRange(1, 299999);

        // then
        Assert.assertEquals(-1208400000, range.getStart());
        Assert.assertEquals(-1207200001, range.getStop());

        // when
        range = gran.deriveRange(1, 1200000);

        // then
        Assert.assertEquals(1200000, range.getStart());
        Assert.assertEquals(2399999, range.getStop());

        // when
        range = gran.deriveRange(1, 1200001);

        // then
        Assert.assertEquals(1200000, range.getStart());
        Assert.assertEquals(2399999, range.getStop());

        // when
        range = gran.deriveRange(1, 2399999);

        // then
        Assert.assertEquals(1200000, range.getStart());
        Assert.assertEquals(2399999, range.getStop());

        // when
        range = gran.deriveRange(1, 2400000);

        // then
        Assert.assertEquals(1200000, range.getStart());
        Assert.assertEquals(2399999, range.getStop());

        // when
        range = gran.deriveRange(1, 2400001);

        // then
        Assert.assertEquals(1200000, range.getStart());
        Assert.assertEquals(2399999, range.getStop());
    }

    @Test
    public void deriveRangeSixtyMinuteFirstSlot() {
        // given
        Granularity gran = Granularity.MIN_60;
        Range range;

        // when
        range = gran.deriveRange(0, 0);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3599999, range.getStop());

        // when
        range = gran.deriveRange(0, 1);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3599999, range.getStop());

        // when
        range = gran.deriveRange(0, 3599999);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3599999, range.getStop());

        // when
        range = gran.deriveRange(0, 3600000);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3599999, range.getStop());

        // when
        range = gran.deriveRange(0, 3600001);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(3599999, range.getStop());
    }

    @Test
    public void deriveRangeSixtyMinuteSecondSlot() {
        // given
        Granularity gran = Granularity.MIN_60;
        Range range;

        // when
        range = gran.deriveRange(1, 0);

        // then
        Assert.assertEquals(-1206000000, range.getStart());
        Assert.assertEquals(-1202400001, range.getStop());

        // when
        range = gran.deriveRange(1, 1);

        // then
        Assert.assertEquals(-1206000000, range.getStart());
        Assert.assertEquals(-1202400001, range.getStop());

        // when
        range = gran.deriveRange(1, 899999);

        // then
        Assert.assertEquals(-1206000000, range.getStart());
        Assert.assertEquals(-1202400001, range.getStop());

        // when
        range = gran.deriveRange(1, 3600000);

        // then
        Assert.assertEquals(3600000, range.getStart());
        Assert.assertEquals(7199999, range.getStop());

        // when
        range = gran.deriveRange(1, 3600001);

        // then
        Assert.assertEquals(3600000, range.getStart());
        Assert.assertEquals(7199999, range.getStop());

        // when
        range = gran.deriveRange(1, 7199999);

        // then
        Assert.assertEquals(3600000, range.getStart());
        Assert.assertEquals(7199999, range.getStop());

        // when
        range = gran.deriveRange(1, 7200000);

        // then
        Assert.assertEquals(3600000, range.getStart());
        Assert.assertEquals(7199999, range.getStop());

        // when
        range = gran.deriveRange(1, 7200001);

        // then
        Assert.assertEquals(3600000, range.getStart());
        Assert.assertEquals(7199999, range.getStop());
    }

    @Test
    public void deriveRange240MinuteFirstSlot() {
        // given
        Granularity gran = Granularity.MIN_240;
        Range range;

        // when
        range = gran.deriveRange(0, 0);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(14399999, range.getStop());

        // when
        range = gran.deriveRange(0, 1);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(14399999, range.getStop());

        // when
        range = gran.deriveRange(0, 14399999);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(14399999, range.getStop());

        // when
        range = gran.deriveRange(0, 14400000);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(14399999, range.getStop());

        // when
        range = gran.deriveRange(0, 14400001);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(14399999, range.getStop());
    }

    @Test
    public void deriveRange240MinuteSecondSlot() {
        // given
        Granularity gran = Granularity.MIN_240;
        Range range;

        // when
        range = gran.deriveRange(1, 0);

        // then
        Assert.assertEquals(-1195200000, range.getStart());
        Assert.assertEquals(-1180800001, range.getStop());

        // when
        range = gran.deriveRange(1, 1);

        // then
        Assert.assertEquals(-1195200000, range.getStart());
        Assert.assertEquals(-1180800001, range.getStop());

        // when
        range = gran.deriveRange(1, 14399999);

        // then
        Assert.assertEquals(-1195200000, range.getStart());
        Assert.assertEquals(-1180800001, range.getStop());

        // when
        range = gran.deriveRange(1, 14400000);

        // then
        Assert.assertEquals(14400000, range.getStart());
        Assert.assertEquals(28799999, range.getStop());

        // when
        range = gran.deriveRange(1, 14400001);

        // then
        Assert.assertEquals(14400000, range.getStart());
        Assert.assertEquals(28799999, range.getStop());

        // when
        range = gran.deriveRange(1, 28799999);

        // then
        Assert.assertEquals(14400000, range.getStart());
        Assert.assertEquals(28799999, range.getStop());

        // when
        range = gran.deriveRange(1, 28800000);

        // then
        Assert.assertEquals(14400000, range.getStart());
        Assert.assertEquals(28799999, range.getStop());

        // when
        range = gran.deriveRange(1, 28800001);

        // then
        Assert.assertEquals(14400000, range.getStart());
        Assert.assertEquals(28799999, range.getStop());
    }

    @Test
    public void deriveRange1440MinuteFirstSlot() {
        // given
        Granularity gran = Granularity.MIN_1440;
        Range range;

        // when
        range = gran.deriveRange(0, 0);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(86399999, range.getStop());

        // when
        range = gran.deriveRange(0, 1);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(86399999, range.getStop());

        // when
        range = gran.deriveRange(0, 86399999);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(86399999, range.getStop());

        // when
        range = gran.deriveRange(0, 86400000);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(86399999, range.getStop());

        // when
        range = gran.deriveRange(0, 86400001);

        // then
        Assert.assertEquals(0, range.getStart());
        Assert.assertEquals(86399999, range.getStop());
    }

    @Test
    public void deriveRange1440MinuteSecondSlot() {
        // given
        Granularity gran = Granularity.MIN_1440;
        Range range;

        // when
        range = gran.deriveRange(1, 0);

        // then
        Assert.assertEquals(-1123200000, range.getStart());
        Assert.assertEquals(-1036800001, range.getStop());

        // when
        range = gran.deriveRange(1, 1);

        // then
        Assert.assertEquals(-1123200000, range.getStart());
        Assert.assertEquals(-1036800001, range.getStop());

        // when
        range = gran.deriveRange(1, 86399999);

        // then
        Assert.assertEquals(-1123200000, range.getStart());
        Assert.assertEquals(-1036800001, range.getStop());

        // when
        range = gran.deriveRange(1, 86400000);

        // then
        Assert.assertEquals(86400000, range.getStart());
        Assert.assertEquals(172799999, range.getStop());

        // when
        range = gran.deriveRange(1, 86400001);

        // then
        Assert.assertEquals(86400000, range.getStart());
        Assert.assertEquals(172799999, range.getStop());

        // when
        range = gran.deriveRange(1, 172799999);

        // then
        Assert.assertEquals(86400000, range.getStart());
        Assert.assertEquals(172799999, range.getStop());

        // when
        range = gran.deriveRange(1, 172800000);

        // then
        Assert.assertEquals(86400000, range.getStart());
        Assert.assertEquals(172799999, range.getStop());

        // when
        range = gran.deriveRange(1, 172800001);

        // then
        Assert.assertEquals(86400000, range.getStart());
        Assert.assertEquals(172799999, range.getStop());
    }

    @Test
    public void slotFromFiner() throws GranularityException {
        Assert.assertEquals(0, Granularity.MIN_5.slotFromFinerSlot(0));
        Assert.assertEquals(1, Granularity.MIN_5.slotFromFinerSlot(1));
        Assert.assertEquals(4031, Granularity.MIN_5.slotFromFinerSlot(4031));
        Assert.assertEquals(4032, Granularity.MIN_5.slotFromFinerSlot(4032));

        Assert.assertEquals(0, Granularity.MIN_20.slotFromFinerSlot(0));
        Assert.assertEquals(0, Granularity.MIN_20.slotFromFinerSlot(1));
        Assert.assertEquals(0, Granularity.MIN_20.slotFromFinerSlot(2));
        Assert.assertEquals(0, Granularity.MIN_20.slotFromFinerSlot(3));
        Assert.assertEquals(1, Granularity.MIN_20.slotFromFinerSlot(4));
        Assert.assertEquals(1006, Granularity.MIN_20.slotFromFinerSlot(4027));
        Assert.assertEquals(1007, Granularity.MIN_20.slotFromFinerSlot(4028));
        Assert.assertEquals(1007, Granularity.MIN_20.slotFromFinerSlot(4029));
        Assert.assertEquals(1007, Granularity.MIN_20.slotFromFinerSlot(4030));
        Assert.assertEquals(1007, Granularity.MIN_20.slotFromFinerSlot(4031));
        Assert.assertEquals(1008, Granularity.MIN_20.slotFromFinerSlot(4032));

        Assert.assertEquals(0, Granularity.MIN_60.slotFromFinerSlot(0));
        Assert.assertEquals(0, Granularity.MIN_60.slotFromFinerSlot(1));
        Assert.assertEquals(0, Granularity.MIN_60.slotFromFinerSlot(2));
        Assert.assertEquals(1, Granularity.MIN_60.slotFromFinerSlot(3));
        Assert.assertEquals(1, Granularity.MIN_60.slotFromFinerSlot(4));
        Assert.assertEquals(1, Granularity.MIN_60.slotFromFinerSlot(5));
        Assert.assertEquals(2, Granularity.MIN_60.slotFromFinerSlot(6));
        Assert.assertEquals(333, Granularity.MIN_60.slotFromFinerSlot(1001));
        Assert.assertEquals(334, Granularity.MIN_60.slotFromFinerSlot(1002));
        Assert.assertEquals(334, Granularity.MIN_60.slotFromFinerSlot(1003));
        Assert.assertEquals(334, Granularity.MIN_60.slotFromFinerSlot(1004));
        Assert.assertEquals(335, Granularity.MIN_60.slotFromFinerSlot(1005));
        Assert.assertEquals(335, Granularity.MIN_60.slotFromFinerSlot(1006));
        Assert.assertEquals(335, Granularity.MIN_60.slotFromFinerSlot(1007));
        Assert.assertEquals(336, Granularity.MIN_60.slotFromFinerSlot(1008));

        Assert.assertEquals(0, Granularity.MIN_240.slotFromFinerSlot(0));
        Assert.assertEquals(0, Granularity.MIN_240.slotFromFinerSlot(1));
        Assert.assertEquals(0, Granularity.MIN_240.slotFromFinerSlot(2));
        Assert.assertEquals(0, Granularity.MIN_240.slotFromFinerSlot(3));
        Assert.assertEquals(1, Granularity.MIN_240.slotFromFinerSlot(4));
        Assert.assertEquals(1, Granularity.MIN_240.slotFromFinerSlot(5));
        Assert.assertEquals(1, Granularity.MIN_240.slotFromFinerSlot(6));
        Assert.assertEquals(1, Granularity.MIN_240.slotFromFinerSlot(7));
        Assert.assertEquals(2, Granularity.MIN_240.slotFromFinerSlot(8));
        Assert.assertEquals(81, Granularity.MIN_240.slotFromFinerSlot(327));
        Assert.assertEquals(82, Granularity.MIN_240.slotFromFinerSlot(328));
        Assert.assertEquals(82, Granularity.MIN_240.slotFromFinerSlot(329));
        Assert.assertEquals(82, Granularity.MIN_240.slotFromFinerSlot(330));
        Assert.assertEquals(82, Granularity.MIN_240.slotFromFinerSlot(331));
        Assert.assertEquals(83, Granularity.MIN_240.slotFromFinerSlot(332));
        Assert.assertEquals(83, Granularity.MIN_240.slotFromFinerSlot(333));
        Assert.assertEquals(83, Granularity.MIN_240.slotFromFinerSlot(334));
        Assert.assertEquals(83, Granularity.MIN_240.slotFromFinerSlot(335));
        Assert.assertEquals(84, Granularity.MIN_240.slotFromFinerSlot(336));

        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(0));
        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(1));
        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(2));
        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(3));
        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(4));
        Assert.assertEquals(0, Granularity.MIN_1440.slotFromFinerSlot(5));
        Assert.assertEquals(1, Granularity.MIN_1440.slotFromFinerSlot(6));
        Assert.assertEquals(12, Granularity.MIN_1440.slotFromFinerSlot(77));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(78));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(79));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(80));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(81));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(82));
        Assert.assertEquals(13, Granularity.MIN_1440.slotFromFinerSlot(83));
        Assert.assertEquals(14, Granularity.MIN_1440.slotFromFinerSlot(84));
    }

    @Test(expected = GranularityException.class)
    public void slotFromFinerWithFullThrowsException() throws GranularityException {
        // when
        Granularity.FULL.slotFromFinerSlot(0);
        // then
        // the exception is thrown
    }

    @Test
    public void slotFromFinerDoesNotWrap() throws GranularityException {
        Assert.assertEquals(4031, Granularity.MIN_20.slotFromFinerSlot(16124));
        Assert.assertEquals(4031, Granularity.MIN_20.slotFromFinerSlot(16125));
        Assert.assertEquals(4031, Granularity.MIN_20.slotFromFinerSlot(16126));
        Assert.assertEquals(4031, Granularity.MIN_20.slotFromFinerSlot(16127));
        Assert.assertEquals(4032, Granularity.MIN_20.slotFromFinerSlot(16128));
    }
}
