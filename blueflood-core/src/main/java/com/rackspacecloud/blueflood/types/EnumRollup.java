package com.rackspacecloud.blueflood.types;

/**
 * Created by tilo on 8/12/15.
 */
public class EnumRollup implements Rollup {
    String name;
    EnumValueRollup valueRollup;

    private class EnumValueRollup {
        String valueName;
        int value;

        public EnumValueRollup(String valueName) {
            this.valueName = valueName;
            this.value = 1;
        }
    }

    public EnumRollup(String name, String valueName) {
        this.name = name;
        this.valueRollup = new EnumValueRollup(valueName);
    }

    @Override
    public Boolean hasData() {
        return null;
    }

    @Override
    public RollupType getRollupType() {
        return null;
    }
}
