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

import java.util.Iterator;

public class RangeTest {

    @Test
    public void testGetStartAndStop() {
        Range myRange = new Range(1, 2);

        Assert.assertEquals(1, myRange.getStart());
        Assert.assertEquals(2, myRange.getStop());
    }

    @Test
    public void testEquals() {
        Range myRange = new Range(1, 2);
        Range myRange2 = new Range(1, 2);
        Range myRange3 = new Range(2, 3);
        Average avg = new Average(1, 2.0);

        Assert.assertFalse(myRange.equals(avg));
        Assert.assertFalse(myRange.equals(myRange3));
        Assert.assertTrue(myRange.equals(myRange2));
    }

    @Test
    public void testToString() {
        Range myRange = new Range(1, 3);

        Assert.assertEquals("1:3 (2)", myRange.toString());
    }

    @Test
    public void testIntervalRangeIteratorRemoveNotSupported() {
        Iterable<Range> myRanges = Range.rangesForInterval(Granularity.MIN_20, 1200000, 1200000);
        Iterator<Range> myRangeIterator = myRanges.iterator();

        Assert.assertTrue(myRangeIterator.hasNext());

        if(myRangeIterator.hasNext()) {
            try {
                myRangeIterator.remove();
                Assert.fail("Never should have gotten here");
            }
            catch (RuntimeException e) {
                Assert.assertEquals("Not supported", e.getMessage());
            }
        }
    }

    @Test
    public void testBadIntervalRangeIterator() {
        try {
            Iterable<Range> myRanges = Range.getRangesToRollup(Granularity.MIN_1440, 1, 300000);
            Assert.fail("Never should have gotten here");
        }
        catch (GranularityException e) {
            Assert.assertEquals("Nothing coarser than metrics_1440m", e.getMessage());
        }

    }
}
