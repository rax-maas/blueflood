/*
 * Copyright 2014 Rackspace
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

/**
 * This class describes the various metrics data types that Blueflood
 * supports.
 *
 * The DataType of a metric determines the:
 * <ul>
 *     <li>(de)serializer used to write/read to/from Cassandra</li>
 *     <li>{@link com.rackspacecloud.blueflood.outputs.formats.MetricData}'s Type,
 *     for query output</li>
 * </ul>
 */
public class DataType {
    private final String type;

    // todo: we need to get rid of this and have a static method that returns the singleton instances below.
    public DataType(String type) {
        this.type = type;
    }

    // In the past, we supported String and Boolean DataType.
    // But those have been removed due to scalability issues.
    // So for now, we only support Numeric.

    /**
     * All metrics containing {@link Number} will get this type
     */
    public final static DataType NUMERIC = new DataType("N");

    public static DataType getMetricType(Object metricValue) {
        if (metricValue instanceof Number) {
            return NUMERIC;
        } else {
            throw new RuntimeException("Unknown metric value type " + metricValue.getClass());
        }
    }

    public static boolean isNumericMetric(Object metricValue) {
        final DataType metricType = getMetricType(metricValue);
        return metricType == DataType.NUMERIC;
    }

    public static boolean isKnownMetricType(DataType incoming) {
        return incoming.equals(NUMERIC);
    }

    @Override
    public String toString() {
        return type;
    }

    public boolean equals(DataType other) {
        return type.equals(other.type);
    }
}
