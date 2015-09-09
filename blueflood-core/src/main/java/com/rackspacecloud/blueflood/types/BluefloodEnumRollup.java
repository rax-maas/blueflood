package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.utils.Util;

import java.io.IOException;
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

    public static BluefloodEnumRollup buildRollupFromEnumRollups(Points<BluefloodEnumRollup> input) throws IOException {
        BluefloodEnumRollup enumRollup = new BluefloodEnumRollup();
        for (Points.Point<BluefloodEnumRollup> point : input.getPoints().values()) {
            BluefloodEnumRollup pointData = point.getData();
            Map<Long, Long> incomingHashedEnums = pointData.getHashedEnumValuesWithCounts();
            Map<Long, Long> currentHashedEnums = enumRollup.getHashedEnumValuesWithCounts();

            for (Long hash : incomingHashedEnums.keySet()) {
                long count = incomingHashedEnums.get(hash);
                if (currentHashedEnums.containsKey(hash)) {
                    long count1 = currentHashedEnums.get(hash);
                    count+=count1;
                }
                enumRollup.getHashedEnumValuesWithCounts().put(hash, count);
            }
        }

        return enumRollup;
    }

}
