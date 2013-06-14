package com.cloudkick.blueflood.service;

import java.util.Collection;

public interface ZKBasedShardLockManagerMBean {
    public Collection<Integer> getHeldShards();
    public Collection<Integer> getUnheldShards();
    public Collection<Integer> getErrorShards();
    
    public long getMinLockHoldTimeMillis();
    public void setMinLockHoldTimeMillis(long millis);
    
    public long getLockDisinterestedTimeMillis();
    public void setLockDisinterestedTimeMillis(long millis);
    
    public void forceLockScavenge();
    public long getSecondsSinceLastScavenge();
    public String getZkConnectionStatus();
    
    public boolean release(int shard);
    public boolean acquire(int shard);
}
