package com.cloudkick.blueflood.service;


public class NoOpShardLockManager implements ShardLockManager {
    
    public void addShard(int shard) {}
    
    public void removeShard(int shard) {}

    public boolean canWork(int shard) {
        return true;
    }
}
