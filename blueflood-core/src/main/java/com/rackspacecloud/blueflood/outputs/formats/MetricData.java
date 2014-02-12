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

public class MetricData {
    private final Points data;
    private final String unit;
    private final String type;

    public MetricData(Points points, String unit, String type) {
        this.data = points;
        this.unit = unit;
        this.type = type;
    }

    public Points getData() {
        return data;
    }

    public String getUnit() {
        return unit;
    }

    public String getType() {
        return type;
    }
}
