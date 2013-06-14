package com.cloudkick.blueflood.rollup;

import com.cloudkick.blueflood.types.Average;
import com.cloudkick.blueflood.types.Range;
import junit.framework.TestCase;
import junit.framework.Assert;

import java.util.Iterator;

public class RangeTest extends TestCase {

    public void testGetStartAndStop() {
        Range myRange = new Range(1, 2);

        assertEquals(1, myRange.getStart());
        assertEquals(2, myRange.getStop());
    }

    public void testEquals() {
        Range myRange = new Range(1, 2);
        Range myRange2 = new Range(1, 2);
        Range myRange3 = new Range(2, 3);
        Average avg = new Average(1, 2.0);

        assertFalse(myRange.equals(avg));
        assertFalse(myRange.equals(myRange3));
        assertTrue(myRange.equals(myRange2));
    }

    public void testToString() {
        Range myRange = new Range(1, 3);

        assertEquals("1:3 (2)", myRange.toString());
    }

    public void testIntervalRangeIteratorRemoveNotSupported() {
        Iterable<Range> myRanges = Range.rangesForInterval(Granularity.MIN_20, 1200000, 1200000);
        Iterator<Range> myRangeIterator = myRanges.iterator();

        assertTrue(myRangeIterator.hasNext());

        if(myRangeIterator.hasNext()) {
            try {
                myRangeIterator.remove();
                Assert.fail("Never should have gotten here");
            }
            catch (RuntimeException e) {
                assertEquals("Not supported", e.getMessage());
            }
        }
    }

    public void testBadIntervalRangeIterator() {
        try {
            Iterable<Range> myRanges = Range.getRangesToRollup(Granularity.MIN_1440, 1, 300000);
            Assert.fail("Never should have gotten here");
        }
        catch (RuntimeException e) {
            assertEquals("Nothing coarser than metrics_1440m", e.getMessage());
        }

    }
}
