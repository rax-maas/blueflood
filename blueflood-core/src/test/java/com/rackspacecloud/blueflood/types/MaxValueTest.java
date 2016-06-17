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
import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MaxValueTest {
    private MaxValue max;

    @Before
    public void setUp() {
        max = new MaxValue();
    }

    @Test
    public void fullResInitialDoubleSetsValue() {

        // when
        max.handleFullResMetric(123.45d);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(123.45d, max.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialLongSetsValue() {

        // when
        max.handleFullResMetric(123L);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(123L, max.toLong());
    }

    @Test
    public void fullResInitialIntegerSetsValue() {

        // when
        max.handleFullResMetric((int)123);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(123L, max.toLong());
    }

    @Test(expected = RuntimeException.class)
    public void fullResInitialFloatThrowsException() {

        // when
        max.handleFullResMetric(123f);

        // then
        // the exception is thrown
    }

    @Test
    public void fullResInitialDoubleThenLesserDouble() {

        // given
        max.handleFullResMetric(123.45d);

        // when
        max.handleFullResMetric(122.45d);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(123.45d, max.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialDoubleThenGreaterDouble() {

        // given
        max.handleFullResMetric(123.45d);

        // when
        max.handleFullResMetric(124.45d);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(124.45d, max.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialDoubleThenLesserLong() {

        // given
        max.handleFullResMetric(123.45d);

        // when
        max.handleFullResMetric(122L);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(123.45d, max.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialDoubleThenGreaterLong() {

        // given
        max.handleFullResMetric(123.45d);

        // when
        max.handleFullResMetric(124L);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(124L, max.toLong());
    }

    @Test
    public void fullResInitialLongThenLesserDouble() {

        // given
        max.handleFullResMetric(123L);

        // when
        max.handleFullResMetric(122.45d);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(123L, max.toLong());
    }

    @Test
    public void fullResInitialLongThenGreaterDouble() {

        // given
        max.handleFullResMetric(123L);

        // when
        max.handleFullResMetric(124.45d);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(124.45d, max.toDouble(), 0.00001d);
    }

    @Test
    public void fullResInitialLongThenLesserLong() {

        // given
        max.handleFullResMetric(123L);

        // when
        max.handleFullResMetric(122L);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(123L, max.toLong());
    }

    @Test
    public void fullResInitialLongThenGreaterLong() {

        // given
        max.handleFullResMetric(123L);

        // when
        max.handleFullResMetric(124L);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(124L, max.toLong());
    }



    @Test
    public void rollupInitialFloatingPointSetsValue() {

        // given
        double value = 123.45d;
        IBaseRollup rollup = mock(IBaseRollup.class);
        MaxValue other = new MaxValue(value);
        doReturn(other).when(rollup).getMaxValue();

        // when
        max.handleRollupMetric(rollup);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(value, max.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialNonFloatingPointSetsValue() {

        // given
        long value = 123L;
        IBaseRollup rollup = mock(IBaseRollup.class);
        MaxValue other = new MaxValue(value);
        doReturn(other).when(rollup).getMaxValue();

        // when
        max.handleRollupMetric(rollup);
        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(123L, max.toLong());
    }

    @Test
    public void rollupInitialDoubleThenLesserDouble() {

        // given
        double value1 = 123.45d;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(value1, max.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialDoubleThenGreaterDouble() {

        // given
        double value1 = 123.45d;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(value2, max.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialDoubleThenLesserLong() {

        // given
        double value1 = 123.45d;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(value1, max.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialDoubleThenGreaterLong() {

        // given
        double value1 = 123.45d;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(value2, max.toLong());
    }

    @Test
    public void rollupInitialLongThenLesserDouble() {

        // given
        long value1 = 123L;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(value1, max.toLong());
    }

    @Test
    public void rollupInitialLongThenGreaterDouble() {

        // given
        long value1 = 123L;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertTrue(max.isFloatingPoint());
        assertEquals(value2, max.toDouble(), 0.00001d);
    }

    @Test
    public void rollupInitialLongThenLesserLong() {

        // given
        long value1 = 123L;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(value1, max.toLong());
    }

    @Test
    public void rollupInitialLongThenGreaterLong() {

        // given
        long value1 = 123L;
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value1)).when(rollup1).getMaxValue();

        max.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
        doReturn(new MaxValue(value2)).when(rollup2).getMaxValue();

        // when
        max.handleRollupMetric(rollup2);

        // then
        assertFalse(max.isFloatingPoint());
        assertEquals(value2, max.toLong());
    }

    @Test
    public void returnsTheCorrectStatType() {

        // expect
        assertEquals(Constants.MAX, max.getStatType());
    }
}
