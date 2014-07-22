/**
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

public class DiscoveryResult {
    private final String metricName;
    private final String unit;
    private final String tenantId;

    public DiscoveryResult(String tenantId, String name, String unit) {
        this.tenantId = tenantId;
        this.metricName = name;
        this.unit = unit;
    }

    public String getTenantId() {
        return tenantId;
    }
    public String getMetricName() {
        return metricName;
    }
    public String getUnit() {
        return unit;
    }
    @Override
    public String toString() {
        return "Result [tenantId=" + tenantId + ", metricName=" + metricName + ", unit=" + unit + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((metricName == null) ? 0 : metricName.hashCode());
        result = prime * result + ((unit == null) ? 0 : unit.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (!getClass().equals(obj.getClass())) {
            return false;
        }
        return equals((DiscoveryResult) obj);
    }
    public boolean equals(DiscoveryResult other) {
        if (this == other) {
            return true;
        }
        return metricName.equals(other.metricName) && unit.equals(other.unit) && tenantId.equals(other.tenantId);
    }
}
