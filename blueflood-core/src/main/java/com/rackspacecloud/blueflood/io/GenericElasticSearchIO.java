package com.rackspacecloud.blueflood.io;

import java.util.List;
import java.util.Map;

public interface GenericElasticSearchIO {
    public void insert(String tenant, List<Map<String, Object>> metrics) throws Exception;
    public List<Map<String, Object>> search(String tenant, Map<String, List<String>> query) throws Exception;
}
