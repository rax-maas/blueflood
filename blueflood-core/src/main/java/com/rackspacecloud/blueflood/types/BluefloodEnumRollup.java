package com.rackspacecloud.blueflood.types;

import java.util.*;

public class BluefloodEnumRollup implements Rollup {
    private Map<String, Long> stringEnumValues2Count = new HashMap<String, Long>();
    private Map<Long,Long> hashedEnumValues2Count = new HashMap<Long, Long>();

    public BluefloodEnumRollup withEnumValue(String valueName, Long value) {
        this.stringEnumValues2Count.put(valueName, value);
        this.hashedEnumValues2Count.put((long) valueName.hashCode(), value);
        return this;
    }

    public BluefloodEnumRollup withHashedEnumValue(Long hashedEnumValue, Long value) {
        this.hashedEnumValues2Count.put(hashedEnumValue, value);
        return this;
    }

    @Override
    public Boolean hasData() {
        return true;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.ENUM;
    }

    public int getCount() {
        return hashedEnumValues2Count.size();
    }

    public Map<Long, Long> getHashedEnumValuesWithCounts() {
        return this.hashedEnumValues2Count;
    }

    public Map<String,Long> getStringEnumValuesWithCounts() { return this.stringEnumValues2Count; }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluefloodEnumRollup)) {
            return false;
        }
        BluefloodEnumRollup other = (BluefloodEnumRollup)obj;
        return hashedEnumValues2Count.equals(other.hashedEnumValues2Count);
    }

}
