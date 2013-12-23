package com.rackspacecloud.blueflood.types;

public enum RollupType {
    STATSD_COUNTER,
    STATSD_TIMER,
    STATSD_SET,
    STATSD_GAUGE,
    BF_HISTOGRAMS,
    BF_BASIC;
    
    public static final String CACHE_KEY = "rollup.type";
    
    public static final RollupType[] SIMPLE_TYPES = new RollupType[] {STATSD_COUNTER, STATSD_SET, STATSD_GAUGE, BF_BASIC};
    
    public static RollupType fromString(String s) {
        if (s == null)
            return RollupType.BF_BASIC;
        
        try {
            return RollupType.valueOf(s.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return RollupType.BF_BASIC;
        }
    }
}
