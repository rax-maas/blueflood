package com.cloudkick.blueflood.types;

import com.cloudkick.blueflood.rollup.Granularity;

import java.util.Map;
import java.util.TreeMap;

public class Points<T> {
    private Map<Long, Point> points; // Map of timestamp to Point

    private Points() {
        this.points = new TreeMap<Long, Point>();
    }

    public void add(Point<T> point) {
        points.put(point.getTimestamp(), point);
    }

    public Map<Long, Point> getPoints() {
        return points;
    }


    private static Points<Object> newFullResolutionPoints() {
        return new Points<Object>();
    }

    private static Points<Rollup> newRollupPoints() {
        return new Points<Rollup>();
    }

    public static Points create(Granularity gran) {
        if (gran == Granularity.FULL) {
            return newFullResolutionPoints();
        }

        return newRollupPoints();
    }

    public static class Point<T> {
        private final T data;
        private final long timestamp;

        public Point(long timestamp, T data) {
            this.timestamp = timestamp;
            this.data = data;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public T getData() {
            return data;
        }
    }
}
