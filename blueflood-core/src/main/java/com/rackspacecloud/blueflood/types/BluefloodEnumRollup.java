package com.rackspacecloud.blueflood.types;

import java.util.*;

public class BluefloodEnumRollup implements Rollup {
    private Set<String> rawEnumValues = new HashSet<String>();
    private Map<Long,Long> hashedEnum2Value = new HashMap<Long, Long>();

    public BluefloodEnumRollup withEnumValue(String valueName, Long value) {
        this.rawEnumValues.add(valueName);
        this.hashedEnum2Value.put((long) valueName.hashCode(), value);
        return this;
    }

    public BluefloodEnumRollup withHashedEnumValue(Long hashedEnumValue, Long value) {
        this.hashedEnum2Value.put(hashedEnumValue, value);
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
        return hashedEnum2Value.size();
    }

    public Map<Long, Long> getHashes() {
        return this.hashedEnum2Value;
    }

    public Set<String> getRawValues() { return this.rawEnumValues; }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluefloodEnumRollup)) {
            return false;
        }
        BluefloodEnumRollup other = (BluefloodEnumRollup)obj;
        return hashedEnum2Value.equals(other.hashedEnum2Value);
    }

}
