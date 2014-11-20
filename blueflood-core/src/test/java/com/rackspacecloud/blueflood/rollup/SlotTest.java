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
import com.rackspacecloud.blueflood.types.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SlotTest {
    @Test
    public void testRangeIteratorFullAnd5m() throws Exception {
        Set<Range> expectedRanges = new HashSet<Range>();
        expectedRanges.add(new Range(0, 299999));
        expectedRanges.add(new Range(300000, 599999));
        expectedRanges.add(new Range(600000, 899999));
        expectedRanges.add(new Range(900000, 1199999));
        
        // FULL and 5m have the same rollup semantics.
        for (Granularity g : new Granularity[] { Granularity.FULL, Granularity.MIN_5}) {
            Set<Range> actualRanges = new HashSet<Range>();
            for (Range time : Range.getRangesToRollup(g, 200000, 1000000)) {
                actualRanges.add(time);
                verifySingleSlot(time, g);
            }
            Assert.assertEquals(expectedRanges, actualRanges);
        }
    }
    
    @Test
    public void testRangeIterator20m() throws Exception {
        Set<Range> expectedRanges = makeRanges(Granularity.MIN_20, 3600000, 33);
        Set<Range> actualRanges = new HashSet<Range>();
        int baseMillis = 6500000;
        int hrs = 10;
        int endMillis = baseMillis + 3600000 * hrs;
        for (Range time : Range.getRangesToRollup(Granularity.MIN_20, baseMillis, endMillis)) {
            actualRanges.add(time);
            verifySingleSlot(time, Granularity.MIN_20);
        }
        Assert.assertEquals(expectedRanges, actualRanges);
    }

    @Test
    public void testRangeMapper60m() throws Exception {
        int baseMillis = 6500000;
        int hrs = 10;
        int endMillis = baseMillis + 3600000 * hrs;
        //Map of every 60m(coarser gran) in this time range, mapped to iterable of 20m sub-ranges that get rolled up
        Map<Range, Iterable<Range>> retMap = Range.mapFinerRanges(Granularity.MIN_60, new Range(baseMillis, endMillis));
        Assert.assertEquals(retMap.entrySet().size(), 11);
        for(Map.Entry<Range,Iterable<Range>> entry : retMap.entrySet()) {
            Range coarserSubRange = entry.getKey();
            int iterValCount = 0;
            Iterable<Range> subranges = entry.getValue();
            for (Range subrange : subranges) {
                if(iterValCount == 0) {
                    //Start point of coarser range is equal to start point of 1st 20m sub-range
                    Assert.assertEquals(coarserSubRange.getStart(), subrange.getStart());
                }
                iterValCount++;
                if(iterValCount == 3) {
                    Assert.assertEquals(coarserSubRange.getStop() - 1, subrange.getStop());
                }
            }
            //Every 60m range gets divided into 3 20m sub-ranges
            Assert.assertEquals(iterValCount, 3);
        }
    }
    
    @Test
    public void testRangeIterator60m() throws Exception {
        Set<Range> expectedRanges = makeRanges(Granularity.MIN_60, 1334577600000L, 72);
        Set<Range> actualRanges = new HashSet<Range>();
        long baseMillis = 1334582854000L; // nearly Mon Apr 16 06:26:52 PDT 2012
        int hrs = 70;
        long endMillis = baseMillis + 3600000 * hrs;
        for (Range time : Range.getRangesToRollup(Granularity.MIN_60, baseMillis, endMillis)) {
            actualRanges.add(time);
            verifySingleSlot(time, Granularity.MIN_60);
        }
        Assert.assertEquals(expectedRanges, actualRanges);
    }
    
    @Test
    public void testRangeIterator240m() throws Exception {
        Set<Range> expectedRanges = makeRanges(Granularity.MIN_240, 1334534400000L, 66);
        Set<Range> actualRanges = new HashSet<Range>();
        long baseMillis = 1334582854000L; // nearly Mon Apr 16 06:26:52 PDT 2012
        int hrs = 240; // 10 days.
        long endMillis = baseMillis + 3500000 * hrs; // todo: should be 3600000?
        for (Range time : Range.getRangesToRollup(Granularity.MIN_240, baseMillis, endMillis)) {
            actualRanges.add(time);
            verifySingleSlot(time, Granularity.MIN_240);    
        }
        Assert.assertEquals(expectedRanges, actualRanges);
    } 
    
    // there is no testRangeIterator1440m because range iteration isn't defined for that granularity because there
    // is no granularity that is more coarse.
    
    // ensure that every point of time within a given range is in the same slot as the beginning of the range.
    private void verifySingleSlot(Range r, Granularity g) {
        int init = g.slot(r.start);
        for (long time = r.start; time <= r.stop; time += 1000)
            Assert.assertEquals(init, g.slot(time));
    }
    
    // create a collection of consecutive ranges.
    private static Set<Range> makeRanges(Granularity g, long startMillis, int count) {
        Set<Range> set = new HashSet<Range>();
        long millis = startMillis;
        for (int i = 0; i < count; i++) {
            long end = millis + g.milliseconds();
            set.add(new Range(millis, end - 1));
            millis = end;
        }
        return set;
    }
    
    @Test
    public void testSlotCalculationsRawAnd5m() {
        final long now = 1331650343000L;
        final long slot = 3634;
        Assert.assertEquals(slot, Granularity.millisToSlot(now));
        
        // 5 mins now should be in the next slot.
        Assert.assertEquals(slot + 1, Granularity.millisToSlot(now + Granularity.MILLISECONDS_IN_SLOT));
        Assert.assertEquals(slot + 1, Granularity.FULL.slot(now + Granularity.MILLISECONDS_IN_SLOT));
        Assert.assertEquals(slot + 1, Granularity.MIN_5.slot(now + Granularity.MILLISECONDS_IN_SLOT));
        
        // should repeat after however many seconds are in NUMBER_OF_SLOTS * MILLISECONDS_PER_SLOT
        Assert.assertEquals(slot, Granularity.millisToSlot(now + (Granularity.FULL.numSlots() * Granularity.MILLISECONDS_IN_SLOT)));
        Assert.assertEquals(slot, Granularity.FULL.slot(now + (Granularity.FULL.numSlots() * Granularity.MILLISECONDS_IN_SLOT)));
        Assert.assertEquals(slot, Granularity.MIN_5.slot(now + (Granularity.FULL.numSlots() * Granularity.MILLISECONDS_IN_SLOT)));
        
        // test the very end of the cycle.
        long endOfCycle = now + ((Granularity.FULL.numSlots() - slot - 1) * Granularity.MILLISECONDS_IN_SLOT); // basically the number of secs to get to slot == 4095
        Assert.assertEquals(Granularity.FULL.numSlots() - 1, Granularity.millisToSlot(endOfCycle));
        Assert.assertEquals(Granularity.FULL.numSlots() - 1, Granularity.FULL.slot(endOfCycle));
        Assert.assertEquals(Granularity.MIN_5.numSlots() - 1, Granularity.MIN_5.slot(endOfCycle));
        
        // adding 300s (one slot) should wrap back around to zero.
        Assert.assertEquals(0, Granularity.millisToSlot(endOfCycle + Granularity.MILLISECONDS_IN_SLOT));
        Assert.assertEquals(0, Granularity.FULL.slot(endOfCycle + Granularity.MILLISECONDS_IN_SLOT));
        Assert.assertEquals(0, Granularity.MIN_5.slot(endOfCycle + Granularity.MILLISECONDS_IN_SLOT));
    }

    @Test
    public void testSlotRelationships() {
        final long now = 1331650343000L;
        final long slot = 3634;
        Assert.assertEquals(slot, Granularity.FULL.slot(now));
        Assert.assertEquals(slot, Granularity.MIN_5.slot(now));
     
        Assert.assertEquals(Granularity.FULL.numSlots(), Granularity.MIN_5.numSlots());
        Assert.assertEquals(Granularity.FULL.numSlots() / 4, Granularity.MIN_20.numSlots());
        Assert.assertEquals(Granularity.FULL.numSlots() / 12, Granularity.MIN_60.numSlots());
        Assert.assertEquals(Granularity.FULL.numSlots() / 48, Granularity.MIN_240.numSlots());
        Assert.assertEquals(Granularity.FULL.numSlots() / 288, Granularity.MIN_1440.numSlots());
        
        // make sure there are no remainders for the minute ratios
        Assert.assertTrue(Granularity.FULL.numSlots() % 4 == 0);
        Assert.assertTrue(Granularity.FULL.numSlots() % 12 == 0);
        Assert.assertTrue(Granularity.FULL.numSlots() % 48 == 0);
        Assert.assertTrue(Granularity.FULL.numSlots() % 288 == 0);
    } 

    @Test
    public void testStaticSameAsFull() {
        final long baseMillis = 1333635148000L; // some point during 5 April 2012.
        final long endMillis = baseMillis + (1000 * 60 * 60 * 48); // +48hrs
        Assert.assertEquals(Granularity.millisToSlot(baseMillis), Granularity.FULL.slot(baseMillis));
        Assert.assertEquals(Granularity.millisToSlot(endMillis), Granularity.FULL.slot(endMillis));
        
    }
    
    @Test
    // make sure the same kind of tests still pass at coarser granularities.
    public void testCoarseSlotCalculations() {
        final long now = 1334582854000L;
        Granularity[] granularities = new Granularity[] {Granularity.MIN_20, Granularity.MIN_60, Granularity.MIN_240, Granularity.MIN_1440};
        int[] initialSlots = new int[] {328, 109, 27, 4};
        
        for (int i = 0; i < initialSlots.length; i++) {
            final long slot = initialSlots[i];
            final Granularity gran = granularities[i]; 
            
            Assert.assertEquals(slot, gran.slot(now));
            
            // next slot should increment by 1.
            Assert.assertEquals(slot + 1, gran.slot(now + gran.milliseconds()));
            
            // should repeat.
            Assert.assertEquals(slot, gran.slot(now + gran.milliseconds() * gran.numSlots()));
            
            // very end of cycle.
            long endOfCycle = now + (gran.numSlots() - slot - 1) * gran.milliseconds();
            Assert.assertEquals(gran.numSlots() - 1, gran.slot(endOfCycle));
            
            // adding seconds() should wrap back to zero.
            Assert.assertEquals(0, gran.slot(endOfCycle + gran.milliseconds()));
        }
    }

    @Test(expected=GranularityException.class)
    public void testSlotFromFinerSlotThrowsAtFull() throws Throwable {
        try {
            Granularity.FULL.slotFromFinerSlot(123);
        } catch (RuntimeException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testSlotFromFinerSlot() {
        // i.e, slot 144 for a 5m is == slot 36 of 20m ( 144 / (20/5)), slot 12 at 60m, slot 3 at 240m, etc
        try {
            Assert.assertEquals(256, Granularity.MIN_5.slotFromFinerSlot(256));

            Assert.assertEquals(35, Granularity.MIN_20.slotFromFinerSlot(143));
            Assert.assertEquals(36, Granularity.MIN_20.slotFromFinerSlot(144));
            Assert.assertEquals(36, Granularity.MIN_20.slotFromFinerSlot(145));
            Assert.assertEquals(36, Granularity.MIN_20.slotFromFinerSlot(146));
            Assert.assertEquals(36, Granularity.MIN_20.slotFromFinerSlot(147));
            Assert.assertEquals(37, Granularity.MIN_20.slotFromFinerSlot(148));

            Assert.assertEquals(12, Granularity.MIN_60.slotFromFinerSlot(36));
            Assert.assertEquals(3, Granularity.MIN_240.slotFromFinerSlot(12));
            Assert.assertEquals(2, Granularity.MIN_1440.slotFromFinerSlot(13));
        } catch (GranularityException e) {
            Assert.assertNull("GranularityException seen on non-full-res Granularity", e);
        }
    }

    
    @Test
    public void testRangeDerivation() {
        for (Granularity gran : Granularity.granularities()) {
            long now = 1334582854000L;
            int nowSlot = gran.slot(now);
            now = gran.snapMillis(now);
            Range nowRange = new Range(now, now + gran.milliseconds() - 1);
            Assert.assertEquals(nowRange, gran.deriveRange(nowSlot, now));
            
            Range prevRange = gran.deriveRange(nowSlot - 1, now);
            Assert.assertEquals(gran.milliseconds(), nowRange.start - prevRange.start);
            
            // incrementing nowSlot forces us to test slot wrapping.
            Range wayBeforeRange = gran.deriveRange(nowSlot + 1, now);
            Assert.assertEquals(gran.numSlots() - 1, (nowRange.start - wayBeforeRange.start) / gran.milliseconds());
        }
    }
}
