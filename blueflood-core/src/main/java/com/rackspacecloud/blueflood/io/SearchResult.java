package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;

public class SearchResult {
    private final Locator locator;
    private final String unit;

    public SearchResult(String tenantId, String name, String unit) {
        this.locator = Locator.createLocatorFromDbKey(String.format("%s.%s", tenantId, name));
        this.unit = unit;
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
    @Override
    public String toString() {
        return "SearchResult [tenantId=" + locator.getTenantId() + ", metricName=" + locator.getMetricName() + ", unit=" + unit + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + locator.hashCode();
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

        return result;
    }
}
