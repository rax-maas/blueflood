package com.cloudkick.blueflood.types;

public class Locator {
    private static String metricTokenSeparator = ",";

    private String stringRep = null;

    public Locator() {
        // Left empty
    }

    public Locator(String metricName) throws IllegalArgumentException {
        setMetricName(metricName);
    }

    protected void setMetricName(String metricName) throws IllegalArgumentException {
        this.stringRep = metricName;
    }

    protected boolean isValidDBKey(String dbKey, String delim) {
        return dbKey.contains(delim);
    }

    public String toString() {
        return stringRep;
    }

    public String getAccountId() {
        return stringRep.split(metricTokenSeparator)[0];
    }
    
    public String getMetricName() {
        return stringRep.substring(stringRep.indexOf(metricTokenSeparator)+1);
    }

    public boolean equals(Locator other) {
        return stringRep.equals(other.toString());
    }

    public static Locator createLocatorFromAccountIdAndName(String accountId, String name) {
        return new Locator(String.format("%s,%s", accountId, name));
    }
}
