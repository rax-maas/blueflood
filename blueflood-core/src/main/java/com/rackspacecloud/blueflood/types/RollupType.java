package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.rollup.Granularity;

public enum RollupType {
    COUNTER,
    TIMER,
    SET,
    GAUGE,
    ENUM,
    BF_BASIC,
    NOT_A_ROLLUP;

    public static final RollupType[] SIMPLE_TYPES = new RollupType[] {COUNTER, SET, GAUGE, BF_BASIC};

    public static RollupType fromString(String s) {
        if (s == null || s.length() == 0)
            return RollupType.BF_BASIC;
        
        try {
            return RollupType.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return RollupType.BF_BASIC;
        }
    }

    public static RollupType fromRollup(Rollup value) {
        if (value instanceof BluefloodSetRollup)
            return RollupType.SET;
        else if (value instanceof BluefloodTimerRollup)
            return RollupType.TIMER;
        else if (value instanceof BluefloodCounterRollup)
            return RollupType.COUNTER;
        else if (value instanceof BluefloodGaugeRollup)
            return RollupType.GAUGE;
        else if  (value instanceof Metric)
            return RollupType.BF_BASIC;
        else if (value instanceof SimpleNumber)
            return RollupType.NOT_A_ROLLUP;
        else if (value instanceof BluefloodEnumRollup) {
            return RollupType.ENUM;
        }
        else
            throw new Error(String.format("Cannot discern RollupType from %s", value.getClass().getSimpleName()));
    }

    /**
     *  Get RollupType from Rollup class type.
     *
     * @param rollupTypeClass
     * @return RollupType
     */
    public static RollupType fromRollupTypeClass( Class<? extends Rollup> rollupTypeClass )  {

        if (rollupTypeClass.equals(SimpleNumber.class)
                || rollupTypeClass.equals( BasicRollup.class)
                || rollupTypeClass.equals( String.class )) {
            return RollupType.BF_BASIC;
        }
        else if (rollupTypeClass.equals(BluefloodCounterRollup.class)) {
            return RollupType.COUNTER;
        }
        else if (rollupTypeClass.equals(BluefloodSetRollup.class)) {
            return RollupType.SET;
        }
        else if (rollupTypeClass.equals(BluefloodTimerRollup.class)) {
            return RollupType.TIMER;
        }
        else if (rollupTypeClass.equals(BluefloodGaugeRollup.class)) {
            return RollupType.GAUGE;
        }
        else if (rollupTypeClass.equals(BluefloodEnumRollup.class)) {
            return RollupType.ENUM;
        }
        else
            throw new Error(String.format("Cannot discern RollupType from %s", rollupTypeClass.getSimpleName()));
    }

    // derive the class of the type. This will be used to determine which serializer is used.
    public static Class<? extends Rollup> classOf(RollupType type, Granularity gran) {
        if (type == RollupType.COUNTER)
            return BluefloodCounterRollup.class;
        else if (type == RollupType.TIMER)
            return BluefloodTimerRollup.class;
        else if (type == RollupType.SET)
            return BluefloodSetRollup.class;
        else if (type == RollupType.GAUGE)
            return BluefloodGaugeRollup.class;
        else if (type == RollupType.BF_BASIC && gran == Granularity.FULL)
            return SimpleNumber.class;
        else if (type == RollupType.BF_BASIC && gran != Granularity.FULL)
            return BasicRollup.class;
        else if(type == RollupType.ENUM)
            return BluefloodEnumRollup.class;
        else
            throw new IllegalArgumentException(String.format("Unexpected type/gran combination: %s, %s", type, gran));
    }
}
