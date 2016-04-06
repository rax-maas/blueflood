package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import static org.junit.Assert.*;

public class PointsTest {

    @Test
    public void newObjectIsEmpty() {

        // when
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        // then
        assertTrue(points.isEmpty());
        assertEquals(0, points.getPoints().size());
    }

    @Test
    public void addIncreasesCount() {

        // given
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);

        // when
        points.add(point);

        // then
        assertFalse(points.isEmpty());
        assertEquals(1, points.getPoints().size());
    }

    @Test(expected=IllegalStateException.class)
    public void getDataClassOnEmptyObjectThrowsException() {

        // given
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        // when
        Class actual = points.getDataClass();

        // then the exception is thrown
    }

    @Test
    public void getDataClassGetsClass() {

        // given
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        SimpleNumber item = new SimpleNumber(42);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);
        points.add(point);

        // when
        Class actual = points.getDataClass();

        // then
        assertSame(SimpleNumber.class, actual);
    }
}
