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

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import sun.security.pkcs11.Secmod;

import java.util.List;

public class MetricData {
    private final Points data;
    private final String unit;
    private final Locator locator;
    private final Type type;
    private static DiscoveryIO discoveryIO = ModuleLoader.getInstance().loadElasticIOModule();

    public MetricData(Points points, Type type, Locator locator) {
        this.data = points;
        this.type = type;
        this.locator = locator;
        this.unit = retrieveUnits();
    }

    public Points getData() {
        return data;
    }

    public String getUnit() {
        return this.unit;
    }

    public String getType() {
        return type.toString();
    }

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

    private String retrieveUnits(){
        List<SearchResult> results = null;
        try {
            results = discoveryIO.search(locator.getTenantId(), locator.getMetricName());
        } catch (Exception e) {
            return "UNKNOWN";
        }
        return results.get(0).getUnit();
    }

    public static void setDiscoveryIO(DiscoveryIO dio) {
        discoveryIO = dio;
    }
}
