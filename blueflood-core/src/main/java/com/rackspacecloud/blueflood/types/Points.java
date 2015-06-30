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

import java.util.Map;
import java.util.TreeMap;

public class Points<T> {
    private Map<Long, Point<T>> points; // Map of timestamp to Point

    public Points() {
        this.points = new TreeMap<Long, Point<T>>();
    }

    public void add(Point<T> point) {
        points.put(point.getTimestamp(), point);
    }

    public Map<Long, Point<T>> getPoints() {
        return points;
    }

    public boolean isEmpty() {
        return points.isEmpty();
    }
    
    public Class getDataClass() {
        if (points.size() == 0)
            throw new IllegalStateException("");
        return points.values().iterator().next().data.getClass();
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

        @Override
        public int hashCode() {
            return (int)(timestamp ^ (timestamp >>> 32)) ^ data.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Point))
                return false;
            try {
                Point<T> other = (Point<T>)obj;
                return other.getTimestamp() == this.getTimestamp()
                        && other.getData().equals(this.getData());
            } catch (ClassCastException ex) {
                // not a Point<T>, but a Point<X> instead?
                return false;
            }
        }
    }
}
