package com.rackspacecloud.blueflood.types;

import java.util.HashMap;
import java.util.Map;

public class EnumRollup implements Rollup {
    private Map<String, Long> en2Value = new HashMap<String, Long>();

    public EnumRollup withEnumValue(String valueName, Long value) {
        this.en2Value.put(valueName, value);
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
        return en2Value.size();
    }

    public Map<String, Long> getHashes() {
        return this.en2Value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof EnumRollup)) {
            return false;
        }
        EnumRollup other = (EnumRollup)obj;
        return en2Value.equals(other.en2Value);
    }

}
