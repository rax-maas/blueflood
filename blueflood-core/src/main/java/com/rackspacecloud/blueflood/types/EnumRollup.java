package com.rackspacecloud.blueflood.types;

import java.util.HashMap;
import java.util.Map;

public class EnumRollup implements Rollup {
    private Map<Integer, Number> en2Value = new HashMap<Integer, Number>();

    public EnumRollup withObject(Integer valueNameHashcode, Number value) {
        this.en2Value.put(valueNameHashcode, value);
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

    public Map<Integer, Number> getHashes() {
        return this.en2Value;
    }

}
