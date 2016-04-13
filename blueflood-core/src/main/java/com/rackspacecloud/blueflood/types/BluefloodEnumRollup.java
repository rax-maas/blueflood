package com.rackspacecloud.blueflood.types;

import java.io.IOException;
import java.util.*;

public class BluefloodEnumRollup implements Rollup {
    private Map<String, Long> stringEnumValues2Count = new HashMap<String, Long>();
    private Map<Long,Long> hashedEnumValues2Count = new HashMap<Long, Long>();

    public BluefloodEnumRollup withEnumValue(String valueName) {
        return this.withEnumValue(valueName, 1L);
    }

    public BluefloodEnumRollup withEnumValue(String valueName, Long incomingCount) {
        long existingCount = 0;
        if (this.stringEnumValues2Count.containsKey(valueName)) {
            existingCount = this.stringEnumValues2Count.get(valueName);
        }
        this.stringEnumValues2Count.put(valueName, existingCount+incomingCount);

        return this.withHashedEnumValue((long)valueName.hashCode(), incomingCount);
    }

    public BluefloodEnumRollup withHashedEnumValue(Long hashedEnumValue, Long incomingCount) {
        long existingHashcount = 0;
        if (this.hashedEnumValues2Count.containsKey(hashedEnumValue)) {
            existingHashcount = this.hashedEnumValues2Count.get(hashedEnumValue);
        }
        this.hashedEnumValues2Count.put((long) hashedEnumValue, incomingCount+existingHashcount);

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

    public int getNumPoints() {
        int enumCount = 0;
        for (long count : hashedEnumValues2Count.values()) {
            enumCount += count;
        }
        return enumCount;
    }

    public Map<Long, Long> getHashedEnumValuesWithCounts() {
        return this.hashedEnumValues2Count;
    }

    public Map<String,Long> getStringEnumValuesWithCounts() {
        return this.stringEnumValues2Count;
    }

    public ArrayList<String> getStringEnumValues() {
        return new ArrayList<String>(this.stringEnumValues2Count.keySet());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluefloodEnumRollup)) {
            return false;
        }
        BluefloodEnumRollup other = (BluefloodEnumRollup)obj;
        return hashedEnumValues2Count.equals(other.hashedEnumValues2Count);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder()
                                    .append(stringEnumValues2Count.toString())
                                    .append(", ")
                                    .append(hashedEnumValues2Count.toString());
        return builder.toString();
    }

    public static BluefloodEnumRollup buildRollupFromEnumRollups(Points<BluefloodEnumRollup> input) throws IOException {
        BluefloodEnumRollup enumRollup = new BluefloodEnumRollup();
        Map<Long, Long> currentHashedEnums = enumRollup.getHashedEnumValuesWithCounts();

        for (Points.Point<BluefloodEnumRollup> point : input.getPoints().values()) {
            BluefloodEnumRollup pointData = point.getData();
            Map<Long, Long> incomingHashedEnums = pointData.getHashedEnumValuesWithCounts();
            for (Long hash : incomingHashedEnums.keySet()) {
                long incomingCount = incomingHashedEnums.get(hash);
                if (currentHashedEnums.containsKey(hash)) {
                    long currentCount = currentHashedEnums.get(hash);
                    incomingCount+=currentCount;
                }
                currentHashedEnums.put(hash, incomingCount);
            }
        }

        return enumRollup;
    }

}
