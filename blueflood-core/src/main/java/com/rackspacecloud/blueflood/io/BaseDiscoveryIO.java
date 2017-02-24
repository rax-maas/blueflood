package com.rackspacecloud.blueflood.io;

import java.util.List;

public interface BaseDiscoveryIO {
    public List<MetricName> getMetricNames(String tenant, String query) throws Exception;
}
