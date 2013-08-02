package com.cloudkick.blueflood.internal;

public class AccountMapEntry {
    private String internalId;
    private String externalId;

    public String getId() {
        return internalId;
    }

    public String getTenantId() {
        return externalId;
    }
}
