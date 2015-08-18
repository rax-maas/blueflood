package com.rackspacecloud.blueflood.types;

import java.util.HashMap;
import java.util.Map;

public class EnumRollup implements Rollup {
    private Map<Long, Long> en2Value = new HashMap<Long, Long>();

    public EnumRollup withObject(Long valueNameHashcode, Long value) {
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

    public Map<Long, Long> getHashes() {
        return this.en2Value;
    }

}
