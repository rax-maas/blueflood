package com.rackspacecloud.blueflood.io;

import java.util.List;

import com.rackspacecloud.blueflood.types.Metric;

public interface DiscoveryIO {
    public void insertDiscovery(List<Metric> metrics) throws Exception;
}
