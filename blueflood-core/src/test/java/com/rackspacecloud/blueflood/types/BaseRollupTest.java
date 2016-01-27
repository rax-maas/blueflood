package com.rackspacecloud.blueflood.types;

/**
 * Holds methods shared by many RollupTest classes
 */
public class BaseRollupTest {

    protected <T> Points<T> asPoints(Class<T> type, long initialTime, long timeDelta, T... values) {
        Points<T> points = new Points<T>();
        long time = initialTime;
        for (T v : values) {
            points.add(new Points.Point<T>(time, v));
            time += timeDelta;
        }
        return points;
    }
}
