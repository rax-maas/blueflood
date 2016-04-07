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

    // adding point with duplicate timestamp replaces previous, e.g. doesn't increase count
    @Test
    public void addWithDuplicateTimestampDoesNotIncreaseCount() {

        // given
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        SimpleNumber item = new SimpleNumber(42);
        SimpleNumber item2 = new SimpleNumber(43);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);
        points.add(point);

        // precondition
        assertEquals(1, points.getPoints().size());

        // when
        points.add(point2);

        // then
        assertFalse(points.isEmpty());
        assertEquals(1, points.getPoints().size());
    }

    @Test
    public void addWithDuplicateTimestampReplacesPrevious() {

        // given
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        SimpleNumber item = new SimpleNumber(42);
        SimpleNumber item2 = new SimpleNumber(43);
        long timestamp = 1234L;
        Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(timestamp, item);
        Points.Point<SimpleNumber> point2 = new Points.Point<SimpleNumber>(timestamp, item2);
        points.add(point);

        // precondition
        assertSame(item, points.getPoints().get(timestamp).getData());

        // when
        points.add(point2);

        // then
        assertSame(item2, points.getPoints().get(timestamp).getData());
    }
}
