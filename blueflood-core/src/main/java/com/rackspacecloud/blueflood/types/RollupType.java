package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.rollup.Granularity;

public enum RollupType {
    COUNTER,
    TIMER,
    SET,
    GAUGE,
    BF_HISTOGRAMS,
    BF_BASIC;

    public static final RollupType[] SIMPLE_TYPES = new RollupType[] {COUNTER, SET, GAUGE, BF_BASIC};
    
    public static RollupType fromString(String s) {
        if (s == null)
            return RollupType.BF_BASIC;
        
        try {
            return RollupType.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return RollupType.BF_BASIC;
        }
    }

    // derive the class of the type. This will be used to determine which serializer is used.
    public static Class<? extends Rollup> classOf(RollupType type, Granularity gran) {
        if (type == RollupType.COUNTER)
            return CounterRollup.class;
        else if (type == RollupType.TIMER)
            return TimerRollup.class;
        else if (type == RollupType.SET)
            return SetRollup.class;
        else if (type == RollupType.GAUGE)
            return GaugeRollup.class;
        else if (type == RollupType.BF_BASIC && gran == Granularity.FULL)
            return SimpleNumber.class;
        else if (type == RollupType.BF_BASIC && gran != Granularity.FULL)
            return BasicRollup.class;
        else
            throw new IllegalArgumentException(String.format("Unexpected type/gran combination: %s, %s", type, gran));
    }
}
