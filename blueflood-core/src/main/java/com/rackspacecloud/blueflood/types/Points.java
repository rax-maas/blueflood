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

import com.rackspacecloud.blueflood.rollup.Granularity;

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

    public boolean isEmpty() {
        return points.isEmpty();
    }

    private static Points<Object> newFullResolutionPoints() {
        return new Points<Object>();
    }

    private static Points<BasicRollup> newRollupPoints() {
        return new Points<BasicRollup>();
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
