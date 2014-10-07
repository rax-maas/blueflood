package com.rackspacecloud.blueflood.io;

import java.util.List;

public interface DiscoveryIO {
    public void insertDiscovery(List<Object> metrics) throws Exception;
    public List<SearchResult> search(String tenant, String query) throws Exception;
}
