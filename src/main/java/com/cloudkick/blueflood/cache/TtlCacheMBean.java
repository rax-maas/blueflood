package com.cloudkick.blueflood.cache;

public interface TtlCacheMBean extends CacheStatsMBean {
    
    public boolean isSafetyMode();
    public void setSafetyThreshold(double d);
    public double getSafetyThreshold();
    
}
