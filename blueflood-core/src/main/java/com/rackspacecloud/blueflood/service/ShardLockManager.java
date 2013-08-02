package com.rackspacecloud.blueflood.service;

public interface ShardLockManager {
    public boolean canWork(int shard);
    public void addShard(int shard);
    public void removeShard(int shard);
}
