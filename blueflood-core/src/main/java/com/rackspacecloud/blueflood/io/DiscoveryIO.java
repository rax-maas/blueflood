package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.IMetric;
import java.util.List;

public interface DiscoveryIO extends MetricNameSearchIO {

    void insertDiscovery(IMetric metric) throws Exception;
    void insertDiscovery(List<IMetric> metrics) throws Exception;
    List<SearchResult> search(String tenant, String query) throws Exception;
    List<SearchResult> search(String tenant, List<String> queries) throws Exception;

}
