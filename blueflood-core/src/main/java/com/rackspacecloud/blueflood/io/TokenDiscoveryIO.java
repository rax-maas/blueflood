package com.rackspacecloud.blueflood.io;


import com.rackspacecloud.blueflood.types.Token;

import java.util.List;

public interface TokenDiscoveryIO {
    public void insertDiscovery(Token token) throws Exception;
    public void insertDiscovery(List<Token> tokens) throws Exception;
    public List<MetricName> getMetricNames(String tenantId, String query) throws Exception;
}
