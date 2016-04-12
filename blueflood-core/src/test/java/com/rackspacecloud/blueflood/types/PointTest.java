package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class PointTest {

    @Test
    public void constructorThrowsNoException() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;

        // when
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // then
        // no exception is thrown
        assertNotNull(point);
    }

    @Test
    public void constructorSetsTimestamp() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;

        // when
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // then
        assertEquals(timestamp, point.getTimestamp());
    }

    @Test
    public void constructorSetsData() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;

        // when
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // then
        assertSame(item, point.getData());
    }

    @Test
    public void pointIsNotEqualToNull() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // when
        boolean isEqual = point.equals(null);

        // then
        assertFalse(isEqual);
    }

    @Test
    public void pointIsNotEqualToObjectOfOtherClass() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // when
        boolean isEqual = point.equals(new Object());

        // then
        assertFalse(isEqual);
    }

    @Test
    public void pointIsNotEqualToOtherPointWithDifferentType() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        float item2 = 42;
        Points.Point<Float> point2 = new Points.Point<Float>(timestamp, item2);

        // when
        boolean isEqual = point.equals(point2);

        // then
        assertFalse(isEqual);
    }

    @Test
    public void pointIsNotEqualIfTimestampsDiffer() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        long timestamp2 = 1235L;
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp2, item);

        // when
        boolean isEqual = point.equals(point2);

        // then
        assertFalse(isEqual);
    }

    @Test
    public void pointIsNotEqualIfDataPointsAreNotEqual() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        SimpleNumber item2 = new SimpleNumber(43);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);

        // when
        boolean isEqual = point.equals(point2);

        // then
        assertFalse(isEqual);
    }

    @Test
    public void pointIsEqualIfDataPointsAreEqualEvenIfNotSameObject() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        SimpleNumber item2 = new SimpleNumber(42);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);

        // when
        boolean isEqual = point.equals(point2);

        // then
        assertTrue(isEqual);
    }

    @Test
    public void pointIsEqualIfDataIsSameObject() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item);

        // when
        boolean isEqual = point.equals(point2);

        // then
        assertTrue(isEqual);
    }

    /**
     * This is not an exhaustive test. There could be instances where two
     * Point<T> objects have different hash codes for equivalent data, e.g. if
     * the {@code hashCode()} method of T is faulty in some way.
     */
    @Test
    public void hashCodeIsEqualForEquivalentData() {

        // This is not an exhaustive test

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        SimpleNumber item2 = new SimpleNumber(42);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);

        // expect
        assertEquals(point.hashCode(), point2.hashCode());
    }

    /**
     * This is not an exhaustive test. There could be instances where two
     * Point<T> objects have identical hash codes for different data, e.g. if
     * the {@code hashCode()} method of T is faulty in some way.
     */
    @Test
    public void hashCodeIsDifferentForDifferentData() {

        // given
        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        SimpleNumber item2 = new SimpleNumber(43);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);

        // expect
        assertThat(point.hashCode(), not(equalTo(point2.hashCode())));
    }
}
