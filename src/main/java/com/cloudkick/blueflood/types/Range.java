package com.cloudkick.blueflood.types;

import com.cloudkick.blueflood.exceptions.GranularityException;
import com.cloudkick.blueflood.rollup.Granularity;

import java.util.Arrays;
import java.util.Iterator;

// typed pair tuple.
public class Range {

    public final long start;
    public final long stop;

    public Range(long l, long r) {
        start = l;
        stop = r;
    }

    public long getStart() {
        return start;
    }

    public long getStop() {
        return stop;
    }

    @Override
    public int hashCode() {
        return (int)(start * 3 + stop * 7);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Range))
            return false;
        else {
            Range other = (Range)obj;
            return other.start == start && other.stop == stop;
        }
    }

    @Override
    public String toString() {
        return String.format("%d:%d (%d)", start, stop, stop-start);
    }
    
    /**
     * given a start and stop time, return an iterator over ranges in *this* granularity that should be rolled up into
     * single points in the next coarser granularity.
     * 
     * Here is an example:  Given start/end (s,e), we need to return all the ranges in Y that correspond to rollup 
     * periods A,B,C.  This involves returning ranges prior to (s) and after (e) that map to parts of (A) and (C) in
     * the coarser rollup.
     * 
     * X [       A      |       B      |       C      |       D      ]
     * Y [    |    | s  |    |    |    |  e |    |    |    |    |    ]\
     * 
     * @param startMillis
     * @param stopMillis
     * @return
     */
    public static Iterable<Range> getRangesToRollup(Granularity g, final long startMillis,
                                                    final long stopMillis) throws GranularityException {
        final long snappedStartMillis = g.coarser().snapMillis(startMillis);
        final long snappedStopMillis = g.coarser().snapMillis(stopMillis + g.coarser().milliseconds());

        return new IntervalRangeIterator(g, snappedStartMillis, snappedStopMillis);
    }
    
    /** return the Ranges for an interval at this granularity
     * @param from start time
     * @param to end time
     * @return Range[]
     */
    public static Iterable<Range> rangesForInterval(Granularity g, final long from, final long to) {
        if (g == Granularity.FULL) {
            return Arrays.asList(new Range(from, to));
        }

        final long snappedStartMillis = g.snapMillis(from);
        final long snappedStopMillis = g.snapMillis(to + g.milliseconds());

        return new IntervalRangeIterator(g, snappedStartMillis, snappedStopMillis);
    }

    /** iterate over Ranges in an interval at a Granularity */
    private static class IntervalRangeIterator implements Iterable<Range> {

        final Granularity granularity;
        final long start;
        final long stop;

        IntervalRangeIterator(Granularity g, long start, long stop) {
            granularity = g;
            this.start = start;
            this.stop = Math.min(stop, System.currentTimeMillis());
        }

        public Iterator<Range> iterator() {
            return new Iterator<Range>() {
                long pos = start;
                public boolean hasNext() {
                    return pos < stop;
                }

                public Range next() {
                    Range res = null;
                    if (pos + granularity.milliseconds() > stop) {
                        res = new Range(pos, stop - 1);
                        pos = stop;
                    } else {
                        long end = granularity.snapMillis(pos + granularity.milliseconds()) - 1;
                        res = new Range(pos, end);
                        pos = end + 1;
                    }
                    return res;
                }

                public void remove() { throw new RuntimeException("Not supported"); }
            };
        };
    }
}