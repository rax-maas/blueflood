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
package com.rackspacecloud.blueflood.outputs.formats;

import com.rackspacecloud.blueflood.types.Points;

/**
 * This class represents the metric data that we return to
 * callers when they call our query APIs.
 */
public class MetricData {
    private static final String NUMBER = "number";
    private final Points data;
    private String unit;

    public MetricData(Points points, String unit) {
        this.data = points;
        this.unit = unit;
    }

    public Points getData() {
        return data;
    }

    public String getUnit() {
        return unit;
    }

    public String getType() {
        // Previously, we have an enum to represent the type
        // (number, boolean, string). Since we remove boolean
        // and string, we now only support number. The string
        // "number" gets returned as type attribute in the
        // query output JSON payload. We return static string
        // "number" here to preserve backwards compatibility
        return NUMBER;
    }

    public void setUnit(String unit) { this.unit = unit; }
}
