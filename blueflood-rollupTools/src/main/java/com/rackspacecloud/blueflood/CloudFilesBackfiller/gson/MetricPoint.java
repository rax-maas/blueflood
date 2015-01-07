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

 * Original author: gdusbabek
 * Modified by: chinmay
 */
package com.rackspacecloud.blueflood.CloudFilesBackfiller.gson;

import java.util.Map;

public class MetricPoint {
    private int metricType;
    private double valueDbl;
    private long valueI64;
    private int valueI32;
    private String valueStr;
    private String unit;
    
    public MetricPoint(Map<String, ?> values) {
        metricType = asInteger(values.get("metricType"));
        valueDbl = asDouble(values.get("valueDbl"));
        valueI64 = asLong(values.get("valueI64"));
        valueI32 = asInteger(values.get("valueI32"));
        valueStr = asString(values.get("valueStr"));
        if (values.get("unitEnum") == null) {
            unit = "unknown";
        } else if (asString(values.get("unitEnum")).toLowerCase().equals("other")) {
            Object otherUnit = values.get("unitOtherStr");
            unit = otherUnit == null ? "unknown" : asString(otherUnit);
        } else {
            unit = asString(values.get("unitEnum"));
        }
    }
    
    public Class<?> getType() {
        if (metricType == 'L' || metricType == 'l')
            return Long.class;
        else if (metricType == 'I' || metricType == 'i')
            return Integer.class;
        else if (metricType == 'n')
            return Double.class;
        else if (metricType == 's' || metricType == 'b')
            return String.class;
        else throw new RuntimeException("Unexpected metric type " + (char)metricType);
    }
    
    public Object getValue() {
        if (metricType == 'L' || metricType == 'l')
            return valueI64;
        else if (metricType == 'I' || metricType == 'i')
            return valueI32;
        else if (metricType == 'n')
            return valueDbl;
        else if (metricType == 's' || metricType == 'b')
            return valueStr;
        else throw new RuntimeException("Unexpected metric type " + (char)metricType);
    }
    
    private static Long asLong(Object o) {
        if (o instanceof Long)
            return (Long)o;
        else if (o instanceof Integer)
            return ((Integer)o).longValue();
        else if (o instanceof Double)
            return (long)((Double) o).doubleValue();
        else if (o instanceof String)
            return Long.parseLong(o.toString());
        else 
            throw new RuntimeException("Cannot convert type");
    }
    
    private static Integer asInteger(Object o) {
        if (o instanceof Long)
            return ((Long) o).intValue();
        else if (o instanceof Integer)
            return (Integer)o;
        else if (o instanceof Double)
            return (int)((Double) o).doubleValue();
        else if (o instanceof String)
            return Integer.parseInt(o.toString());
        else 
            throw new RuntimeException("Cannot convert type");
    }
    
    private static Double asDouble(Object o) {
        if (o instanceof Long)
            return (double)((Long) o).longValue();
        else if (o instanceof Integer)
            return (double)((Integer) o).intValue(); 
        else if (o instanceof Double)
            return (Double)o;
        else if (o instanceof String)
            return Double.parseDouble(o.toString());
        else 
            throw new RuntimeException("Cannot convert type");
    }
    
    private static String asString(Object o) {
        return o == null ? "" : o.toString();
    }

    public String getUnit() {
        return unit;
    }
}
