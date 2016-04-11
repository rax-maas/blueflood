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

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MinValueTest {
    private MinValue min;

    @Before
    public void setUp() {
        min = new MinValue();
    }

    @Test
    public void testMinValueForDoubleMetrics() throws IOException {
        for (double val : TestData.DOUBLE_SRC) {
            min.handleFullResMetric(val);
        }
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(0.0, min.toDouble(), 0);
    }

    @Test
    public void testMinValueForLongMetrics() throws IOException {
        for (long val : TestData.LONG_SRC) {
            min.handleFullResMetric(val);
        }
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(12L, min.toLong());
    }

    @Test
    public void testMinValueWithMixedTypes() throws IOException {
        min.handleFullResMetric(6L);    // long
        min.handleFullResMetric(6.0);   // double
        min.handleFullResMetric(1);     // integer
        min.handleFullResMetric(99.0);  // double

        // The minimum value in the input set is 1 which is of type Long
        Assert.assertTrue(!min.isFloatingPoint());
        // Assert that indeed 1 is the minimum value
        Assert.assertEquals(1, min.toLong());
    }

    @Test
    public void testRollupMin() throws IOException {
        BasicRollup basicRollup1 = new BasicRollup();
        BasicRollup basicRollup2 = new BasicRollup();
        BasicRollup basicRollup3 = new BasicRollup();
        BasicRollup basicRollup4 = new BasicRollup();

        BasicRollup netBasicRollup;

        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(123456789L, new SimpleNumber(5L)));
        input.add(new Points.Point<SimpleNumber>(123456790L, new SimpleNumber(1L)));
        input.add(new Points.Point<SimpleNumber>(123456791L, new SimpleNumber(7L)));
        basicRollup1 = BasicRollup.buildRollupFromRawSamples(input);

        input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(123456789L, new SimpleNumber(9L)));
        input.add(new Points.Point<SimpleNumber>(123456790L, new SimpleNumber(0L)));
        input.add(new Points.Point<SimpleNumber>(123456791L, new SimpleNumber(1L)));
        basicRollup2 = BasicRollup.buildRollupFromRawSamples(input);

        Points<BasicRollup> rollups = new Points<BasicRollup>();
        BasicRollup temp = new BasicRollup();
        temp.getMinValue().setDoubleValue(2.14);
        rollups.add(new Points.Point<BasicRollup>(123456789L, temp));
        temp.getMinValue().setDoubleValue(1.14);
        rollups.add(new Points.Point<BasicRollup>(123456790L, temp));
        basicRollup3 = BasicRollup.buildRollupFromRollups(rollups);

        rollups = new Points<BasicRollup>();
        temp = new BasicRollup();
        temp.getMinValue().setDoubleValue(3.14);
        rollups.add(new Points.Point<BasicRollup>(123456789L, temp));
        temp.getMinValue().setDoubleValue(5.67);
        rollups.add(new Points.Point<BasicRollup>(123456790L, temp));
        basicRollup4 = BasicRollup.buildRollupFromRollups(rollups);

        // handle homegenous metric types and see if we get the right min

        // type long
        rollups = new Points<BasicRollup>();
        rollups.add(new Points.Point<BasicRollup>(123456789L, basicRollup1));
        rollups.add(new Points.Point<BasicRollup>(123456790L, basicRollup2));
        netBasicRollup = BasicRollup.buildRollupFromRollups(rollups);

        MinValue min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());

        // type double
        rollups = new Points<BasicRollup>();
        rollups.add(new Points.Point<BasicRollup>(123456789L, basicRollup3));
        rollups.add(new Points.Point<BasicRollup>(123456790L, basicRollup4));
        netBasicRollup = BasicRollup.buildRollupFromRollups(rollups);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(min.isFloatingPoint());
        Assert.assertEquals(1.14d, min.toDouble(), 0);

        // handle heterogenous metric types and see if we get the right min
        rollups = new Points<BasicRollup>();
        rollups.add(new Points.Point<BasicRollup>(123456789L, basicRollup2));
        rollups.add(new Points.Point<BasicRollup>(123456790L, basicRollup3));
        netBasicRollup = BasicRollup.buildRollupFromRollups(rollups);

        min = netBasicRollup.getMinValue();
        Assert.assertTrue(!min.isFloatingPoint());
        Assert.assertEquals(0L, min.toLong());
    }

    @Test
    public void fullResInitialDoubleSetsValue() {

        // when
        min.handleFullResMetric(123.45d);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(123.45d, min.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialLongSetsValue() {

        // when
        min.handleFullResMetric(123L);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(123L, min.toLong());
    }

    @Test
    public void fullResInitialIntegerSetsValue() {

        // when
        min.handleFullResMetric((int)123);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(123L, min.toLong());
    }

    @Test(expected = RuntimeException.class)
    public void fullResInitialFloatThrowsException() {

        // when
        min.handleFullResMetric(123f);

        // then
        // the exception is thrown
    }

    @Test
    public void fullResInitialDoubleThenLesserDouble() {

        // given
        min.handleFullResMetric(123.45d);

        // when
        min.handleFullResMetric(122.45d);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(122.45d, min.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialDoubleThenGreaterDouble() {

        // given
        min.handleFullResMetric(123.45d);

        // when
        min.handleFullResMetric(124.45d);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(123.45d, min.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialDoubleThenLesserLong() {

        // given
        min.handleFullResMetric(123.45d);

        // when
        min.handleFullResMetric(122L);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(122L, min.toLong());
    }

    @Test
    public void fullResInitialDoubleThenGreaterLong() {

        // given
        min.handleFullResMetric(123.45d);

        // when
        min.handleFullResMetric(124L);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(123.45d, min.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialLongThenLesserDouble() {

        // given
        min.handleFullResMetric(123L);

        // when
        min.handleFullResMetric(122.45d);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(122.45d, min.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialLongThenGreaterDouble() {

        // given
        min.handleFullResMetric(123L);

        // when
        min.handleFullResMetric(124.45d);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(123L, min.toLong());
    }

    @Test
    public void fullResInitialLongThenLesserLong() {

        // given
        min.handleFullResMetric(123L);

        // when
        min.handleFullResMetric(122L);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(122L, min.toLong());
    }

    @Test
    public void fullResInitialLongThenGreaterLong() {

        // given
        min.handleFullResMetric(123L);

        // when
        min.handleFullResMetric(124L);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(123L, min.toLong());
    }



    @Test
    public void rollupInitialFloatingPointSetsValue() {

        // given
        double value = 123.45d;
        IBasicRollup rollup = mock(IBasicRollup.class);
        MinValue other = new MinValue(value);
        doReturn(other).when(rollup).getMinValue();

        // when
        min.handleRollupMetric(rollup);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(value, min.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialNonFloatingPointSetsValue() {

        // given
        long value = 123L;
        IBasicRollup rollup = mock(IBasicRollup.class);
        MinValue other = new MinValue(value);
        doReturn(other).when(rollup).getMinValue();

        // when
        min.handleRollupMetric(rollup);
        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(123L, min.toLong());
    }

    @Test
    public void rollupInitialDoubleThenLesserDouble() {

        // given
        double value1 = 123.45d;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(value2, min.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialDoubleThenGreaterDouble() {

        // given
        double value1 = 123.45d;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(value1, min.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialDoubleThenLesserLong() {

        // given
        double value1 = 123.45d;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(value2, min.toLong());
    }

    @Test
    public void rollupInitialDoubleThenGreaterLong() {

        // given
        double value1 = 123.45d;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(value1, min.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialLongThenLesserDouble() {

        // given
        long value1 = 123L;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertTrue(min.isFloatingPoint());
        assertEquals(value2, min.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialLongThenGreaterDouble() {

        // given
        long value1 = 123L;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(value1, min.toLong());
    }

    @Test
    public void rollupInitialLongThenLesserLong() {

        // given
        long value1 = 123L;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(value2, min.toLong());
    }

    @Test
    public void rollupInitialLongThenGreaterLong() {

        // given
        long value1 = 123L;
        IBasicRollup rollup1 = mock(IBasicRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBasicRollup rollup2 = mock(IBasicRollup.class);
        doReturn(new MinValue(value2)).when(rollup2).getMinValue();

        // when
        min.handleRollupMetric(rollup2);

        // then
        assertFalse(min.isFloatingPoint());
        assertEquals(value1, min.toLong());
    }

    @Test
    public void returnsTheCorrectStatType() {

        // expect
        assertEquals(Constants.MIN, min.getStatType());
    }
}
