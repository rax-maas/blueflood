package com.rackspacecloud.blueflood.cache;

public interface CacheStatsMBean {
    public long getHitCount();
    public double getHitRate();
    public long getMissCount();
    public double getMissRate();
    public long getLoadCount();
    public long getRequestCount();
    public long getTotalLoadTime();
}
