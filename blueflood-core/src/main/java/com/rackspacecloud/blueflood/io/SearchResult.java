package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;
import java.util.ArrayList;

public class SearchResult {
    private final Locator locator;
    private final String unit;
    private final ArrayList<String> enumValues;

    public SearchResult(String tenantId, String name, String unit) {
        this.locator = Locator.createLocatorFromDbKey(String.format("%s.%s", tenantId, name));
        this.unit = unit;
        this.enumValues = null;
    }

    public SearchResult(String tenantId, String name, String unit, ArrayList<String> enumValues) {
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

    public ArrayList<String> getEnumValues() {
        return enumValues;
    }

    @Override
    public String toString() {
        String s = "SearchResult [tenantId=" + locator.getTenantId() + ", metricName=" + locator.getMetricName() + ", unit=" + unit;

        if (enumValues != null) {
            String enumValuesString = "";
            for (String enumValue : enumValues) {
                if (enumValuesString != "") {
                    enumValuesString += ",";
                }
                enumValuesString += enumValue.replace(",", "\\,");
            }
            s += ", enumValues=" + enumValuesString + "]";
        }

        s += "]";
        return s;
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
        boolean result = locator.equals(other.locator);

        if (unit != null) {
            result = result && unit.equals(other.unit);
        }

        ArrayList<String> otherEnumValues = other.getEnumValues();
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
