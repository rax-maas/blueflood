package com.rackspacecloud.blueflood.types;

public enum StatType {
    COUNTER,
    TIMER,
    SET,
    GAUGE,
    UNKNOWN;
    
    public static final String CACHE_KEY = "statsd.type";
    
    public static final StatType[] SIMPLE_TYPES = new StatType[] { COUNTER, SET, GAUGE, UNKNOWN };
    
    public static StatType fromString(String s) {
        if (s == null)
            return StatType.UNKNOWN;
        
        try {
            return StatType.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return StatType.UNKNOWN;
        }
    }
}
