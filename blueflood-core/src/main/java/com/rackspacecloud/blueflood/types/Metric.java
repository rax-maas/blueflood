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


import com.rackspacecloud.blueflood.utils.TimeValue;

public class Metric implements IMetric {
    private final Locator locator;
    private final Object metricValue;
    private final long collectionTime;
    private int ttlSeconds;
    private final Type metricType;
    private final String unit;

    public Metric(Locator locator, Object metricValue, long collectionTime, TimeValue ttl, String unit) {
        this.locator = locator;
        this.metricValue = metricValue;
        this.collectionTime = collectionTime;
        this.metricType = Type.getMetricType(metricValue);
        this.unit = unit;

        setTtl(ttl);
    }

    public Locator getLocator() {
        return locator;
    }

    public Object getValue() {
        return metricValue;
    }

    public Type getType() {
        return metricType;
    }

    public int getTtlInSeconds() {
        return ttlSeconds;
    }

    public long getCollectionTime() {
        return collectionTime;
    }

    public String getUnit() {
        return unit;
    }

    public boolean isNumeric() {
        return Type.isNumericMetric(metricValue);
    }

    public boolean isString() {
        return Type.isStringMetric(metricValue);
    }

    public boolean isBoolean() {
        return Type.isBooleanMetric(metricValue);
    }

    public void setTtl(TimeValue ttl) {
        if (!isValidTTL(ttl.toSeconds())) {
            throw new RuntimeException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttl.toSeconds());
        }

        ttlSeconds = (int) ttl.toSeconds();
    }

    public void setTtlInSeconds(int ttlInSeconds) {
        if (!isValidTTL(ttlInSeconds)) {
            throw new RuntimeException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttlInSeconds);
        }

        ttlSeconds = ttlInSeconds;
    }

    @Override
    public String toString() {
        return String.format("%s:%s:%s:%s:%s", locator.toString(), metricValue, metricType, ttlSeconds, unit == null ? "" : unit.toString());
    }

    private boolean isValidTTL(long ttlInSeconds) {
        return (ttlInSeconds < Integer.MAX_VALUE && ttlInSeconds > 0);
    }

    public static class Type {
        private final String type;

        // todo: we need to get rid of this and have a static method that returns the singleton instances below.
        public Type(String type) {
            this.type = type;
        }

        public final static Type STRING = new Type("S");
        public final static Type INT = new Type("I");
        public final static Type LONG = new Type("L");
        public final static Type DOUBLE = new Type("D");
        public final static Type BOOLEAN = new Type("B");

        public static Type getMetricType(Object metricValue) {
            if (metricValue instanceof String) {
                return STRING;
            } else if (metricValue instanceof Integer) {
                return INT;
            } else if (metricValue instanceof Long) {
                return LONG;
            } else if (metricValue instanceof Double) {
                return DOUBLE;
            } else if (metricValue instanceof Boolean) {
                return BOOLEAN;
            } else {
                throw new RuntimeException("Unknown metric value type");
            }
        }

        public static boolean isNumericMetric(Object metricValue) {
            final Type metricType = getMetricType(metricValue);
            return metricType == Type.INT || metricType == Type.LONG || metricType == Type.DOUBLE;
        }

        public static boolean isStringMetric(Object metricValue) {
            final Type metricType = getMetricType(metricValue);
            return metricType == Type.STRING;
        }

        public static boolean isBooleanMetric(Object metricValue) {
            final Type metricType = getMetricType(metricValue);
            return metricType == Type.BOOLEAN;
        }

        public static boolean isKnownMetricType(Type incoming) {
            return incoming.equals(STRING) || incoming.equals(INT) || incoming.equals(LONG) || incoming.equals(DOUBLE)
                    || incoming.equals(BOOLEAN);
        }

        @Override
        public String toString() {
            return type;
        }

        public boolean equals(Type other) {
            return type.equals(other.type);
        }
    }
}
