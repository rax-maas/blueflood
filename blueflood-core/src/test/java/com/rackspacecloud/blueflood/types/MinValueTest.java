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
        IBaseRollup rollup = mock(IBaseRollup.class);
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
        IBaseRollup rollup = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 122.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        double value2 = 124.45d;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 122L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
        IBaseRollup rollup1 = mock(IBaseRollup.class);
        doReturn(new MinValue(value1)).when(rollup1).getMinValue();

        min.handleRollupMetric(rollup1);

        long value2 = 124L;
        IBaseRollup rollup2 = mock(IBaseRollup.class);
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
