package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.types.Points;

/**
 * Holds some methods shared across many RollupSerializerTest classes
 */
public class BaseRollupSerializerTest {

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
