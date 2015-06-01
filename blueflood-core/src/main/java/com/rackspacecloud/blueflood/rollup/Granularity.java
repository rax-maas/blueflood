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

import com.rackspacecloud.blueflood.cache.ConfigTtlProvider;
import com.rackspacecloud.blueflood.cache.SafetyTtlProvider;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.RollupType;

import java.util.Calendar;

/**
 * 1440m    [ not enough space to show the relationship, but there would be 6 units of the 240m ranges in 1 1440m range.
 * 240m     [                                               |                                               |        ...
 * 60m      [           |           |           |           |           |           |           |           |        ...
 * 20m      [   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |   |...
 * 5m       [||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||...
 * full     [ granularity to the second, but ranges are partitioned the same as in 5m.                               ...
 */
public final class Granularity {
    private static final int GET_BY_POINTS_ASSUME_INTERVAL = Configuration.getInstance().getIntegerProperty(CoreConfig.GET_BY_POINTS_ASSUME_INTERVAL);
    private static final String GET_BY_POINTS_SELECTION_ALGORITHM = Configuration.getInstance().getStringProperty(CoreConfig.GET_BY_POINTS_GRANULARITY_SELECTION);
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

    private static final Granularity[] granularities = new Granularity[] { FULL, MIN_5, MIN_20, MIN_60, MIN_240, MIN_1440 }; // order is important.

    public static final Granularity LAST = MIN_1440;

    private static final Granularity[] rollupGranularities = new Granularity[] { MIN_5, MIN_20, MIN_60, MIN_240, MIN_1440 }; // order is important.
    
    public static final int MAX_NUM_SLOTS = FULL.numSlots() + MIN_5.numSlots() + MIN_20.numSlots() + MIN_60.numSlots() + MIN_240.numSlots() + MIN_1440.numSlots();

    private static SafetyTtlProvider SAFETY_TTL_PROVIDER;

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
        return indexOf(this) > indexOf(other);
    }

    private int indexOf(Granularity gran) {
        for (int i = 0; i < granularities.length; i++) {
            if (gran == granularities[i]) {
                return i;
            }
        }

        throw new RuntimeException("Granularity " + gran.toString() + " not present in granularities list.");
    }

    /**
     * Gets the floor multiple of number of milliseconds in this granularity
     * @param millis
     * @return
     */
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
     * returns the slot for the current granularity based on a supplied slot from the granularity one resolution finer
     * i.e, slot 144 for a 5m is == slot 36 of 20m (because 144 / (20m/5m)), slot 12 at 60m, slot 3 at 240m, etc
     * @param finerSlot
     * @return
     */
    public int slotFromFinerSlot(int finerSlot) throws GranularityException {
        return (finerSlot * numSlots()) / this.finer().numSlots();
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

    /**
     * Return granularity that maps most closely to requested number of points based on
     * provided selection algorithm
     *
     * @param from beginning of interval (millis)
     * @param to end of interval (millis)
     * @param points count of desired data points
     * @return
     */
    public static Granularity granularityFromPointsInInterval(String tenantid, long from, long to, int points) {
        if (from >= to) {
            throw new RuntimeException("Invalid interval specified for fromPointsInInterval");
        }

        double requestedDuration = to - from;

        if (GET_BY_POINTS_SELECTION_ALGORITHM.startsWith("GEOMETRIC"))
            return granularityFromPointsGeometric(tenantid, from, to, requestedDuration, points);
        else if (GET_BY_POINTS_SELECTION_ALGORITHM.startsWith("LINEAR"))
            return granularityFromPointsLinear(requestedDuration, points);
        else if (GET_BY_POINTS_SELECTION_ALGORITHM.startsWith("LESSTHANEQUAL"))
            return granularityFromPointsLessThanEqual(requestedDuration, points);

        return granularityFromPointsGeometric(tenantid, from, to, requestedDuration, points);
    }

    /**
     * Find the granularity in the interval that will yield a number of data points that are
     * closest to the requested points but <= requested points.
     *
     * @param requestedDuration
     * @param points
     * @return
     */
    private static Granularity granularityFromPointsLessThanEqual(double requestedDuration, int points) {
        Granularity gran = granularityFromPointsLinear(requestedDuration, points);

        if (requestedDuration / gran.milliseconds() > points) {
            try {
                gran = gran.coarser();
            } catch (GranularityException e) { /* do nothing, already at 1440m */ }
        }

        return gran;
    }

    /**
     * Find the granularity in the interval that will yield a number of data points that are close to $points
     * in terms of linear distance.
     *
     * @param requestedDuration
     * @param points
     * @return
     */
    private static Granularity granularityFromPointsLinear(double requestedDuration, int points) {
        int closest = Integer.MAX_VALUE;
        int diff = 0;
        Granularity gran = null;

        for (Granularity g : Granularity.granularities()) {
            if (g == Granularity.FULL)
                diff = (int)Math.abs(points - (requestedDuration / GET_BY_POINTS_ASSUME_INTERVAL));
            else
                diff = (int)Math.abs(points - (requestedDuration /g.milliseconds()));
            if (diff < closest) {
                closest = diff;
                gran = g;
            } else {
                break;
            }
        }

        return gran;
    }

    /**
     *
     * Look for the granularity that would generate the density of data points closest to the value desired. For
     * example, if 500 points were requested, it is better to return 1000 points (2x more than were requested)
     * than it is to return 100 points (5x less than were requested). Our objective is to generate reasonable
     * looking graphs.
     *
     * @param requestedDuration (milliseconds)
     */
    private static Granularity granularityFromPointsGeometric(String tenantid, long from, long to, double requestedDuration, int requestedPoints) {
        double minimumPositivePointRatio = Double.MAX_VALUE;
        Granularity gran = null;
        if (SAFETY_TTL_PROVIDER == null) {
            SAFETY_TTL_PROVIDER = SafetyTtlProvider.getInstance();
        }

        for (Granularity g : Granularity.granularities()) {
            long ttl = SAFETY_TTL_PROVIDER.getFinalTTL(tenantid, g);

            if (from < Calendar.getInstance().getTimeInMillis() - ttl) {
                continue;
            }

            // FULL resolution is tricky because we don't know the period of check in question. Assume the minimum
            // period and go from there.
            long period = (g == Granularity.FULL) ? GET_BY_POINTS_ASSUME_INTERVAL : g.milliseconds();
            double providablePoints = requestedDuration / period;
            double positiveRatio;

            // Generate a ratio >= 1 of either (points requested / points provided by this granularity) or the inverse.
            // Think of it as an "absolute ratio". Our goal is to minimize this ratio.
            if (providablePoints > requestedPoints) {
                positiveRatio = providablePoints / requestedPoints;
            } else {
                positiveRatio = requestedPoints / providablePoints;
            }

            if (positiveRatio < minimumPositivePointRatio) {
                minimumPositivePointRatio = positiveRatio;
                gran = g;
            } else {
                break;
            }
        }

        if (gran == null) {
            gran = Granularity.LAST;
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

    @Override
    public String toString() {
        return name();
    }
}
