/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class SearchResultTest {
    final String TENANT_ID = "100001";
    final String METRIC_NAME = "a.b.123";
    final String UNIT = "mm";

    @Test
    public void testCreateSearchResult() {
        SearchResult searchResult = new SearchResult(TENANT_ID, METRIC_NAME, UNIT);
        Assert.assertEquals(TENANT_ID, searchResult.getTenantId());
        Assert.assertEquals(METRIC_NAME, searchResult.getMetricName());
        Assert.assertEquals(UNIT, searchResult.getUnit());

        String expectedString = "SearchResult [tenantId=" + TENANT_ID + ", metricName=" + METRIC_NAME + ", unit=" + UNIT + "]";
        Assert.assertEquals(expectedString, searchResult.toString());
        Assert.assertNotNull(searchResult.hashCode());
    }

    @Test
    public void testEquals() {
        SearchResult result1 = new SearchResult(TENANT_ID, METRIC_NAME, null);
        SearchResult result2 = new SearchResult(TENANT_ID, METRIC_NAME, null);
        Assert.assertTrue("result1 should equal self", result1.equals(result1));
        Assert.assertTrue("result1 should equal result2", result1.equals(result2));
        Assert.assertTrue("result1 should not equal null", !result1.equals(null));

        String METRIC_NAME2 = "metric2";
        SearchResult result3 = new SearchResult(TENANT_ID, METRIC_NAME2, UNIT);
        SearchResult result4 = new SearchResult(TENANT_ID, METRIC_NAME2, UNIT);
        Assert.assertTrue("result3 should equal result4", result3.equals(result4));
        Assert.assertTrue("result1 should not equal result3", !result1.equals(result3));
    }
}
