package com.rackspacecloud.blueflood.types;

import org.apache.commons.lang.StringUtils;

public class Locator {
    private static String metricTokenSeparator = ",";

    private String stringRep = null;
    private String tenantId = null;
    private String metricName = null;

    public Locator() {
        // Left empty
    }

    private Locator(String fullyQualifiedMetricName) throws IllegalArgumentException {
        setStringRep(fullyQualifiedMetricName);
    }

    protected void setStringRep(String rep) throws IllegalArgumentException {
        this.stringRep = rep;
        tenantId = this.stringRep.split(metricTokenSeparator)[0];
        metricName = this.stringRep.substring(this.stringRep.indexOf(metricTokenSeparator)+1);
    }

    protected boolean isValidDBKey(String dbKey, String delim) {
        return dbKey.contains(delim);
    }

    public String toString() {
        return stringRep;
    }

    public String getTenantId() {
        return this.tenantId;
    }
    
    public String getMetricName() {
        return this.metricName;
    }

    public boolean equals(Locator other) {
        return stringRep.equals(other.toString());
    }

    public static Locator createLocatorFromPathComponents(String tenantId, String... parts) throws IllegalArgumentException {
        return new Locator(tenantId + metricTokenSeparator + StringUtils.join(parts, metricTokenSeparator));
    }

    public static Locator createLocatorFromDbKey(String fullyQualifiedMetricName) throws IllegalArgumentException {
        return new Locator(fullyQualifiedMetricName);
    }
}
