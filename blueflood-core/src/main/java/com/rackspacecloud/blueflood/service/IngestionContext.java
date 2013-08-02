package com.rackspacecloud.blueflood.service;

public interface IngestionContext {
    public void update(long millis, int shard);
}
