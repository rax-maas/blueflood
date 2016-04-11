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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;

import java.util.ArrayList;
import java.util.List;

public class SearchResult {
    private final Locator locator;
    private final String unit;
    private final List<String> enumValues;

    public SearchResult(String tenantId, String name, String unit) {
        this.locator = Locator.createLocatorFromDbKey(String.format("%s.%s", tenantId, name));
        this.unit = unit;
        this.enumValues = null;
    }

    public SearchResult(String tenantId, String name, String unit, List<String> enumValues) {
        this.locator = Locator.createLocatorFromDbKey(String.format("%s.%s", tenantId, name));
        this.unit = unit;
        this.enumValues = enumValues;
    }

    public String getTenantId() {
        return locator.getTenantId();
    }

    public String getMetricName() {
        return locator.getMetricName();
    }

    public String getUnit() {
        return unit;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append("SearchResult [tenantId=").append(locator.getTenantId())
                .append(", metricName=").append(locator.getMetricName());

        if (unit != null) {
            sb.append(", unit=").append(unit);
        }

        if (enumValues != null) {
            sb.append(", enumValues=").append(enumValues.toString());
        }

        sb.append("]");

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        // hash code for locator
        result = prime * result + locator.hashCode();

        // hash code for unit if not null
        result = prime * result + ((unit == null) ? 0 : unit.hashCode());

        // hash code for enum values if not null
        result = prime * result + ((enumValues == null) ? 0 : enumValues.hashCode());

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
        return equals((SearchResult) obj);
    }
    
    public boolean equals(SearchResult other) {
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }

        boolean result = locator.equals(other.locator);

        if (unit != null) {
            result = result && unit.equals(other.unit);
        }

        List<String> otherEnumValues = other.getEnumValues();
        if ((enumValues != null) || (otherEnumValues != null)){
            boolean enumValuesEqual;
            if (enumValues != null) {
                enumValuesEqual = enumValues.equals(otherEnumValues);
            }
            else {
                enumValuesEqual = otherEnumValues.equals(enumValues);
            }
            result = result && enumValuesEqual;
        }

        return result;
    }
}
