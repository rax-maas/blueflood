// Copyright 2014 Square, Inc.
package com.rackspacecloud.blueflood.service;

/**
 * A service with a notion of shards under its management.
 *
 * The work is done only against the shards managed by this service.
 * @author Jeeyoung Kim
 */
public interface ShardedService {
    /**
     * Add a shard.
     * @param shard
     */
    public void addShard(int shard);

    /**
     * Remove a shard.
     * @param shard
     */
    public void removeShard(int shard);
}
