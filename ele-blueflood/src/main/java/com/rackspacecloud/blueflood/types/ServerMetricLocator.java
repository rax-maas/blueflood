package com.rackspacecloud.blueflood.types;

// todo: CM_SPECIFIC
public class ServerMetricLocator extends Locator {
    private static final String expectedDelim = ",";
    private static final String persistDelim = "_";

    private final String acctId;
    private final String entityId;
    private final String checkId;
    private final String metric;

    public static ServerMetricLocator createFromTelescopePrimitives(String acctId, String entityId, String checkId,
                                                                    String metric) {
        return new ServerMetricLocator(acctId, entityId, checkId, metric);
    }

    public static ServerMetricLocator createFromDBKey(String dbKey) throws IllegalArgumentException {
        return new ServerMetricLocator(dbKey);
    }

    private ServerMetricLocator(String acctId, String entityId, String checkId, String metric) {
        this.acctId = acctId.replace(expectedDelim, persistDelim);
        this.entityId = entityId.replace(expectedDelim, persistDelim);
        this.checkId = checkId.replace(expectedDelim, persistDelim);
        this.metric = metric.replace(expectedDelim, persistDelim);
        setStringRep(this.buildStringRep());
    }

    private ServerMetricLocator(String locator) throws IllegalArgumentException {
        if (!isValidDBKey(locator, expectedDelim)) {
            throw new IllegalArgumentException("Expected delimiter " + "'" + expectedDelim + "' " + "but got " +
                    locator);
        }

        String[] tokens = locator.split(expectedDelim);
        this.acctId = tokens[0];
        this.entityId = tokens[1];
        this.checkId = tokens[2];
        this.metric = tokens[3];
        setStringRep(this.buildStringRep());
    }

    public String getCheckId() {
        return this.checkId;
    }

    public String getMetric() {
        return this.metric;
    }

    @Override
    public String getAccountId() {
        return this.acctId;
    }
    
    public String getEntityId() {
        return this.entityId;
    }

    private String buildStringRep() {
         return String.format("%s,%s,%s,%s", this.acctId, this.entityId, this.checkId, this.metric);
    }

    public boolean equals(Locator other) {
        return other.toString().equals(toString());
    }

    public String getDBKey() {
        return toString();
    }
}