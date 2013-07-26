package com.cloudkick.blueflood.types;

import org.apache.commons.lang.StringUtils;

public class Locator {
    private static String metricTokenSeparator = ",";

    private String stringRep = null;
    private String accountId = null;
    private String metricName = null;

    public Locator() {
        // Left empty
    }

    private Locator(String fullyQualifiedMetricName) throws IllegalArgumentException {
        setStringRep(fullyQualifiedMetricName);
    }

    protected void setStringRep(String rep) throws IllegalArgumentException {
        this.stringRep = rep;
        accountId = this.stringRep.split(metricTokenSeparator)[0];
        metricName = this.stringRep.substring(this.stringRep.indexOf(metricTokenSeparator)+1);
    }

    protected boolean isValidDBKey(String dbKey, String delim) {
        return dbKey.contains(delim);
    }

    public String toString() {
        return stringRep;
    }

    public String getAccountId() {
        return this.accountId;
    }
    
    public String getMetricName() {
        return this.metricName;
    }

    public boolean equals(Locator other) {
        return stringRep.equals(other.toString());
    }

    public static Locator createLocatorFromPathComponents(String accountId, String... parts) throws IllegalArgumentException {
        return new Locator(accountId + metricTokenSeparator + StringUtils.join(parts, metricTokenSeparator));
    }

    public static Locator createLocatorFromDbKey(String fullyQualifiedMetricName) throws IllegalArgumentException {
        return new Locator(fullyQualifiedMetricName);
    }
}
