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
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 1440m    [ not enough space to show the relationship, but there would be 6 units of the 240m ranges in 1 1440m rnage.
 * 240m     [                                               |                                               |        ...
 * 60m      [           |           |           |           |           |           |           |           |        ...
 * 20m      [   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |...
 * 5m       [||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||...
 * full     [ granularity to the second, but ranges are partitioned the same as in 5m.                               ...
 */
public class Granularity {
    private static final int MIN_PERIOD_MILLIS = 30000;
    private static int INDEX_COUNTER = 0;
    private static final int BASE_SLOTS_PER_GRANULARITY = 4032; // needs to be a multiple of the GCF of 4, 12, 48, 288.
    public static final int MILLISECONDS_IN_SLOT = 300000;
    private static final int SECS_PER_DAY = 86400;
    
    public static final Granularity FULL = new Granularity("metrics_full", 300000, BASE_SLOTS_PER_GRANULARITY, "full");
    public static final Granularity MIN_5 = new Granularity("metrics_5m", 300000, BASE_SLOTS_PER_GRANULARITY, "5m");
    public static final Granularity MIN_20 = new Granularity("metrics_20m", 1200000, (BASE_SLOTS_PER_GRANULARITY / 4), "20m");
    public static final Granularity MIN_60 = new Granularity("metrics_60m", 3600000, (BASE_SLOTS_PER_GRANULARITY / 12), "60m");
    public static final Granularity MIN_240 = new Granularity("metrics_240m", 14400000, (BASE_SLOTS_PER_GRANULARITY / 48), "240m");
    public static final Granularity MIN_1440 = new Granularity("metrics_1440m", 86400000, (BASE_SLOTS_PER_GRANULARITY / 288), "1440m");
    
    private static final Granularity LAST = MIN_1440;
    
    private static final Granularity[] granularities = new Granularity[] { FULL, MIN_5, MIN_20, MIN_60, MIN_240, MIN_1440 };
    private static final Granularity[] rollupGranularities = new Granularity[] { MIN_5, MIN_20, MIN_60, MIN_240, MIN_1440 };
    
    public static final int MAX_NUM_SLOTS = FULL.numSlots() + MIN_5.numSlots() + MIN_20.numSlots() + MIN_60.numSlots() + MIN_240.numSlots() + MIN_1440.numSlots();
    
    // simple counter for all instances, since there will be very few.
    private final int index;
    
    // name of column family where rollups are kept.
    private final String cf;
    
    // like cf, but shorter.
    private final String shortName;
    
    // number of milliseconds in one slot of this rollup.
    private final int milliseconds;
    
    // number of slots for this granularity.  This number decreases as granularity is more coarse.  Also, the number of
    // minutes indicated by a single slot increases as the number of slots goes down.
    private final int numSlots;
    
    private Granularity(String cf, int milliseconds, int numSlots, String shortName) {
        index = INDEX_COUNTER++;
        this.cf = cf;
        this.milliseconds = milliseconds;
        this.numSlots = numSlots;
        this.shortName = shortName;
    }
    
    // name->column_family
    public String name() { return cf; }
    
    // name->tenant ttl key.
    public String shortName() { return shortName; }
    
    /** @return the number of seconds in one slot range. */
    public int milliseconds() { return milliseconds; }
    
    public int numSlots() { return numSlots; }
    
    // returns the next coarser granularity.
    // FULL -> 5m -> 20m -> 60m -> 240m -> 1440m -> explosion.
    public Granularity coarser() throws GranularityException {
        if (this == LAST) throw new GranularityException("Nothing coarser than " + name());
        return granularities[index + 1];
    }
    
    // opposite of coarser().
    public Granularity finer() throws GranularityException {
        if (this == FULL) throw new GranularityException("Nothing finer than " + name());
        return granularities[index - 1];
    }

    public boolean isCoarser(Granularity other) {
        int myIndex = indexOf(this);
        int otherIndex = indexOf(other);

        if (myIndex != -1 && otherIndex != -1) {
            return myIndex > otherIndex;
        }

        throw new RuntimeException("Invalid granularity comparison, this = " + this.toString()
                + ", other = " + other.toString());
    }

    private int indexOf(Granularity gran) {
        for (int i = 0; i < granularities.length; i++) {
            if (gran == granularities[i]) {
                return i;
            }
        }

        return -1;
    }

    // todo: needs explanation.
    public long snapMillis(long millis) {
        if (this == FULL) return millis;
        else return (millis / milliseconds) * milliseconds;
    }

    /**
     * At full granularity, a slot is 300 continuous seconds.  The duration of a single slot goes up (way up) as
     * granularity is lost.  At the same time the number of slots decreases.
     * @param millis
     * @return
     */
    public int slot(long millis) {
        // the actual slot is 
        int fullSlot = millisToSlot(millis);
        return (numSlots * fullSlot) / BASE_SLOTS_PER_GRANULARITY;
    }

    /**
     * We need to derive ranges (actual times) from slots (which are fixed integers that wrap) when we discover a late
     * slot. These ranges can be derived from a reference point (which is usually something like now).
     * @param slot
     * @param referenceMillis
     * @return
     */
    public Range deriveRange(int slot, long referenceMillis) {
        // referenceMillis refers to the current time in reference to the range we want to generate from the supplied 
        // slot. This implies that the range we wish to return is before slot(reference).  allow for slot wrapping.
        referenceMillis = snapMillis(referenceMillis);
        int refSlot = slot(referenceMillis);
        int slotDiff = slot > refSlot ? (numSlots() - slot + refSlot) : (refSlot - slot);
        long rangeStart = referenceMillis - slotDiff * milliseconds();
        return new Range(rangeStart, rangeStart + milliseconds() - 1);
    }
    
    // return all the locator keys of this slot and its finer children, recursively.
    public Set<String> getChildrenKeys(int slot, int shard) {
        HashSet<String> set = new HashSet<String>();
        try {
            Granularity finer = finer();
            int factor = finer.numSlots() / numSlots();
            // basically this: add all the keys this slot maps to in the finer granularity,
            // then add their children too.
            for (int i = 0; i < factor; i++) {
                int childSlot = slot * factor + i;
                set.add(finer.formatLocatorKey(childSlot, shard));
                set.addAll(finer.getChildrenKeys(childSlot, shard));
            }
            return set;
        } catch (GranularityException ex) {
            return set;
        }
    }
    
    /** iterates over locator keys (gran + slot) for a given time range */
    Iterable<String> locatorKeys(final int shard, final long start, final long stop) {
        return new Iterable<String>() {
            private final int startSlot = slot(snapMillis(start));
            // stop slot is determined by 
            private final int stopSlot = slot(snapMillis(stop + milliseconds)); 
            
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    
                    private int cur = startSlot;
                    
                    public boolean hasNext() {
                        if (startSlot <= stopSlot)
                            return cur < stopSlot;
                        else {
                            if (cur >= startSlot)
                                return cur < numSlots;
                            else
                                return cur < stopSlot;
                        }
                    }

                    public String next() {
                        String v = formatLocatorKey(cur, shard);
                        cur = (cur + 1) % numSlots;
                        return v;
                    }

                    public void remove() { throw new RuntimeException("Not supported"); }
                };
            }
        };
    }

    /** find the granularity in the interval that will yield a number of data points that are close to $points. T
     * The number of data points returned may be less than requested because the next finer resolution would return
     * too many points.
     * @param from beginning of interval (millis)
     * @param to end of interval (millis)
     * @param points count of desired data points
     * @return
     */
    public static Granularity granularityFromPointsInInterval(long from, long to, int points) {
        if (from >= to) {
            throw new RuntimeException("Invalid interval specified for fromPointsInInterval");
        }

        // look for the granularity that would generate the number of data points closest to the value desired.
        // for FULL, assume 1 data p oint every 30s (the min timeout).
        int closest = Integer.MAX_VALUE;
        Granularity gran = null;
        
        for (Granularity g : Granularity.granularities()) {
            int diff = 0;
            // deciding when to use full resolution is tricky because we don't know the period of the check in question.
            // assume the minimum was selected and go from there.
            if (g == Granularity.FULL)
                diff = (int)Math.abs(points - ((to-from)/ MIN_PERIOD_MILLIS));
            else
                diff = (int)Math.abs(points - ((to-from)/g.milliseconds())); 
            if (diff < closest) {
                closest = diff;
                gran = g;
            }
        }
        return gran;
    }

    /** calculate the full/5m slot based on 4032 slots of 300000 milliseconds per slot. */
    static int millisToSlot(long millis) {
        return (int)((millis % (BASE_SLOTS_PER_GRANULARITY * MILLISECONDS_IN_SLOT)) / MILLISECONDS_IN_SLOT);
    }

    @Override
    public int hashCode() {
        return name().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Granularity)) return false;
        else return obj == this;
    }

    public static Granularity[] granularities() { return granularities; }

    public static Granularity[] rollupGranularities() { return rollupGranularities; }
    
    public static Granularity fromString(String s) {
        for (Granularity g : granularities)
            if (g.name().equals(s) || g.shortName().equals(s))
                return g;
        return null;
    }
    
    // this is the key used in metrics_meta and metrics_locator. returns an intern()ed string (there will only be about
    // 5900 of them and I'd like to be able to do == comparisons).
    public String formatLocatorKey(int slot, int shard) {   
        return String.format("%s,%d,%d", name(), slot, shard).intern(); 
    }
    
    // get granularity from a locator key.
    public static Granularity granularityFromKey(String key) {
        for (Granularity g : granularities)
            if (key.startsWith(g.name() + ","))
                return g;
        throw new RuntimeException("Unexpected granularity: " + key);
    }
    
    public static int shardFromKey(String s) {
        return Integer.parseInt(s.substring(s.lastIndexOf(",") + 1));
    }
    
    // get slot from locator key.  todo: needs tests.
    public static int slotFromKey(String s) {
        return Integer.parseInt(s.split(",")[1]);
    }

    @Override
    public String toString() {
        return name();
    }
}
