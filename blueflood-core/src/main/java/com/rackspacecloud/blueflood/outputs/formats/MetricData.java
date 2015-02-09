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

import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.RollupType;

public class MetricData {
    private final Points data;
    private String unit;
    private final Type type;

    public MetricData(Points points, String unit, Type type) {
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
        return type.toString();
    }

    public void setUnit(String unit) { this.unit = unit; }

    public enum Type {
        NUMBER("number"),
        BOOLEAN("boolean"),
        STRING("string"),
        HISTOGRAM("histogram");

        private Type(String s) {
            this.name = s;
        }

        private String name;

        @Override
        public String toString() {
            return name;
        }

        public static Type from(RollupType rollupType, DataType dataType) {
            // We no longer store datatype metadata for Numeric datatypes
            if (dataType == null) {
                return NUMBER;
            }

            if (rollupType == null) {
                rollupType = RollupType.BF_BASIC;
            }

            if (dataType.equals(DataType.STRING)) {
                return STRING;
            } else if (dataType.equals(DataType.BOOLEAN)) {
                return BOOLEAN;
            } else {
                if (rollupType == RollupType.BF_HISTOGRAMS) {
                    return HISTOGRAM;
                }

                return NUMBER;
            }
        }
    }
}
