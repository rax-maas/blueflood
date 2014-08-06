package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricsIndexHandler;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestIndexHandler {
    
    @Test
    public void testSearchResultToJSON() {
        List<SearchResult> results = new ArrayList<SearchResult>();
        results.add(new SearchResult("tenant0", "a.b.c.d.foo", "parsecs"));
        results.add(new SearchResult("tenant1", "a.b.c.d.bar", "furlongs"));

        String searchResultsJson = HttpMetricsIndexHandler.getSerializedJSON(results);
        Assert.assertFalse("[]".equals(searchResultsJson));
    }
}
