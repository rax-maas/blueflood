package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.IMetric;

import java.util.List;

/**
 * Used by {@link com.rackspacecloud.blueflood.utils.ModuleLoaderTest}
 */
public class DummyDiscoveryIO implements DiscoveryIO {

    @Override
    public void insertDiscovery(IMetric metric) throws Exception {

    }

    @Override
    public void insertDiscovery(List<IMetric> metrics) throws Exception {

    }

    @Override
    public List<SearchResult> search(String tenant, String query) throws Exception {
        return null;
    }

    @Override
    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        return null;
    }

    @Override
    public List<MetricToken> getMetricTokens(String tenant, String prefix) throws Exception {
        return null;
    }
}
