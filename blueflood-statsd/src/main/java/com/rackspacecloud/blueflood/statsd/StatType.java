package com.rackspacecloud.blueflood.statsd;

public enum StatType {
    COUNTER,
    TIMER,
    SET,
    GAUGE,
    UNKNOWN;
    
    public static final String CACHE_KEY = "statsd.type";
    
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
