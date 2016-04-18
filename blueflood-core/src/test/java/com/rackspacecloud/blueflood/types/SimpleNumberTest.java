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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleNumberTest {

    @Test
    public void constructorWithIntegerSetsValue() {

        // given
        Object testValue = new Integer(4);

        // when
        SimpleNumber simpleNumber = new SimpleNumber(testValue);

        // then
        assertEquals(testValue, simpleNumber.getValue());
        assertEquals(SimpleNumber.Type.INTEGER, simpleNumber.getDataType());
        assertTrue(simpleNumber.hasData());
    }

    @Test
    public void constructorWithDoubleSetsValue() {

        // given
        Object testValue = new Double(5.0);

        // when
        SimpleNumber simpleNumber = new SimpleNumber(testValue);

        // then
        assertEquals(testValue, simpleNumber.getValue());
        assertEquals(SimpleNumber.Type.DOUBLE, simpleNumber.getDataType());
        assertTrue(simpleNumber.hasData());
    }

    @Test
    public void constructorWithLongSetsValue() {

        // given
        Object testValue = new Long(5L);

        // when
        SimpleNumber simpleNumber = new SimpleNumber(testValue);

        // then
        assertEquals(testValue, simpleNumber.getValue());
        assertEquals(SimpleNumber.Type.LONG, simpleNumber.getDataType());
        assertTrue(simpleNumber.hasData());
    }

    @Test
    public void constructorWithBoxedIntegerSetsValue() {

        // given
        Object testValue = 4;

        // when
        SimpleNumber simpleNumber = new SimpleNumber(testValue);

        // then
        assertEquals(testValue, simpleNumber.getValue());
        assertEquals(SimpleNumber.Type.INTEGER, simpleNumber.getDataType());
        assertTrue(simpleNumber.hasData());
    }

    @Test(expected = NullPointerException.class)
    public void constructorWithNullThrowsException() {

        // when
        SimpleNumber simpleNumber = new SimpleNumber(null);

        // then
        // the exception is thrown
    }

    @Test
    public void constructorWithSimpleNumberSetsValue() {

        // given
        Object testValue = new SimpleNumber(123L);

        // when
        SimpleNumber simpleNumber = new SimpleNumber(testValue);

        // then
        assertEquals(123L, simpleNumber.getValue());
        assertEquals(SimpleNumber.Type.LONG, simpleNumber.getDataType());
        assertTrue(simpleNumber.hasData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorWithFloatThrowsException() {

        // when
        SimpleNumber simpleNumber = new SimpleNumber(123.45f);

        // then
        // the exception is thrown
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorWithOtherTypeThrowsException() {

        // when
        SimpleNumber simpleNumber = new SimpleNumber(new Object());

        // then
        // the exception is thrown
    }

    @Test
    public void toStringWithIntegerPrintsIntegerString() {

        // given
        SimpleNumber sn = new SimpleNumber(123);

        // expect
        assertEquals("123 (int)", sn.toString());
    }

    @Test
    public void toStringWithLongPrintsLongString() {

        // given
        SimpleNumber sn = new SimpleNumber(123L);

        // expect
        assertEquals("123 (long)", sn.toString());
    }

    @Test
    public void toStringWithDoublePrintsDoubleString() {

        // given
        SimpleNumber sn = new SimpleNumber(123.45d);

        // expect
        assertEquals("123.45 (double)", sn.toString());
    }

    @Test
    public void rollupTypeIsNotTypical() {

        // given
        SimpleNumber sn = new SimpleNumber(123.45d);

        // expect
        assertEquals(RollupType.NOT_A_ROLLUP, sn.getRollupType());
    }

    @Test
    public void hashCodeIsValuesHashCode() {

        // given
        Double value = 123.45d;
        SimpleNumber sn = new SimpleNumber(value);

        // expect
        assertEquals(value.hashCode(), sn.hashCode());
    }

    @Test
    public void equalsWithNullReturnsFalse() {

        // given
        SimpleNumber sn = new SimpleNumber(123.45d);

        // expect
        assertFalse(sn.equals(null));
    }

    @Test
    public void equalsWithOtherTypeReturnsFalse() {

        // given
        SimpleNumber sn = new SimpleNumber(123.45d);

        // expect
        assertFalse(sn.equals(Double.valueOf(123.45d)));
    }

    @Test
    public void equalsWithSimpleNumberOfOtherTypeReturnsFalse() {

        // given
        SimpleNumber sn = new SimpleNumber(123);
        SimpleNumber other = new SimpleNumber(123L);

        // expect
        assertFalse(sn.equals(other));
    }

    @Test
    public void equalsWithSimpleNumberOfSameTypeReturnsTrue() {

        // given
        SimpleNumber sn = new SimpleNumber(123);
        SimpleNumber other = new SimpleNumber(123);

        // expect
        assertTrue(sn.equals(other));
    }

    @Test
    public void equalsWithSimpleNumberOfSameBoxedValueReturnsTrue() {

        // given
        Integer value = 123;
        SimpleNumber sn = new SimpleNumber(value);
        SimpleNumber other = new SimpleNumber(value);

        // expect
        assertTrue(sn.equals(other));
    }
}
