package com.rackspacecloud.blueflood.types;

import java.util.HashSet;
import java.util.Set;

public class EnumRollup implements Rollup {
    String name;
    HashSet<Integer> hashes =  new HashSet<Integer>();

    public EnumRollup withObject(String name, String valueName, Number value) {
        this.name = name;
        EnumValueRollup enumValueRollup = new EnumValueRollup(valueName, value);

        hashes.add(enumValueRollup.hashCode());
        return this;
    }

    private class EnumValueRollup {
        String valueName;
        Number value;

        public EnumValueRollup(String valueName, Number value) {
            this.valueName = valueName;
            this.value = value;
        }
    }

    public EnumRollup() {
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
        return hashes.size();
    }

    public Set<Integer> getHashes() {
        return this.hashes;
    }

}
