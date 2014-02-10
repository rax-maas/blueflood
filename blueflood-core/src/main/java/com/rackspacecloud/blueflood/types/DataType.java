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

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    STRING("S"),
    BOOLEAN("B"),
    LONG("L"),
    INT("I"),
    DOUBLE("D"),
    // TODO: We should eventually migrate to using this for all numbers
    NUMBER("N");

    private static Map<String, DataType> lookups;

    static {
        lookups = new HashMap<String, DataType>();
        for (DataType item : DataType.values()) {
            lookups.put(item.getCode(), item);
        }
    }

    private DataType(String code) {
        this.code = code;
    }

    public static DataType getMetricType(Object metricValue) {
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
        final DataType metricType = getMetricType(metricValue);
        return metricType == DataType.INT || metricType == DataType.LONG || metricType == DataType.DOUBLE;
    }

    public static boolean isStringMetric(Object metricValue) {
        final DataType metricType = getMetricType(metricValue);
        return metricType == DataType.STRING;
    }

    public static boolean isBooleanMetric(Object metricValue) {
        final DataType metricType = getMetricType(metricValue);
        return metricType == DataType.BOOLEAN;
    }

    public static boolean isKnownMetricType(DataType incoming) {
        return incoming.equals(STRING) || incoming.equals(INT) || incoming.equals(LONG) || incoming.equals(DOUBLE)
                || incoming.equals(BOOLEAN);
    }

    @Override
    public String toString() {
        return code; // for safety, let's always return code when toString() is called.
    }

    public String getCode() {
        return code;
    }

    public static DataType fromCode(String code) {
        return lookups.get(code);
    }

    public boolean equals(DataType other) {
        return code.equals(other.code);
    }

    private final String code;
}