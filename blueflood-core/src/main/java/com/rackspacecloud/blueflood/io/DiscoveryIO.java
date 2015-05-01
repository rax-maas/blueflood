package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;

import java.util.List;

public interface DiscoveryIO {
    public void insertDiscovery(List<IMetric> metrics) throws Exception;
    public List<SearchResult> search(String tenant, String query) throws Exception;
    public List<SearchResult> search(List<Locator> locators);
}
