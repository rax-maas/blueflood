package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.MetricToken;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.types.IMetric;

import java.util.List;

/**
 * Used by {@link ModuleLoaderTest}
 */
public class DummyDiscoveryIO3 implements DiscoveryIO {

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
