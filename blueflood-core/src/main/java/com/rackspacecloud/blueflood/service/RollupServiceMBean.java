package com.rackspacecloud.blueflood.service;

import java.util.Collection;

public interface RollupServiceMBean {
    void setKeepingServerTime(boolean b);
    boolean getKeepingServerTime();
    void setServerTime(long seconds);
    long getServerTime();
    void setPollerPeriod(long l);
    long getPollerPeriod();
    public int getScheduledSlotCheckCount();
    public void forcePoll();
    
    public int getSecondsSinceLastSlotCheck();
    public int getQueuedRollupCount();
    public int getSlotCheckConcurrency();
    public void setSlotCheckConcurrency(int i);
    
    public int getInFlightRollupCount();
    public int getRollupConcurrency();
    public void setRollupConcurrency(int i);
    
    public boolean getActive();
    public void setActive(boolean b);

    /* shard management methods  */
    public void addShard(final Integer shard);
    public void removeShard(final Integer shard);
    public Collection<Integer> getManagedShards();
    
    public Collection<Integer> getRecentlyScheduledShards();

    public Collection<String> getMetricsState(int shard, int slot);
    public Collection<String> getOldestUnrolledSlotPerGranularity(int shard);
}
