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
        Assert.assertNull(searchResult.getEnumValues());

        String expectedString = "SearchResult [tenantId=" + TENANT_ID + ", metricName=" + METRIC_NAME + ", unit=" + UNIT + "]";
        Assert.assertEquals(expectedString, searchResult.toString());
        Assert.assertNotNull(searchResult.hashCode());
    }

    @Test
    public void testCreateSearchResultWithEnumValues() {
        ArrayList<String> enumValues = new ArrayList<String>();
        enumValues.add("v1");
        enumValues.add("v2");

        SearchResult searchResult = new SearchResult(TENANT_ID, METRIC_NAME, null, enumValues);
        Assert.assertEquals(TENANT_ID, searchResult.getTenantId());
        Assert.assertEquals(METRIC_NAME, searchResult.getMetricName());
        Assert.assertNull(searchResult.getUnit());
        Assert.assertEquals(enumValues, searchResult.getEnumValues());

        String expectedString = "SearchResult [tenantId=" + TENANT_ID + ", metricName=" + METRIC_NAME + ", enumValues=" + enumValues.toString() + "]";
        Assert.assertEquals(expectedString, searchResult.toString());
        Assert.assertNotNull(searchResult.hashCode());
    }

    @Test
    public void testEquals() {
        SearchResult result1 = new SearchResult(TENANT_ID, METRIC_NAME, null);
        SearchResult result2 = new SearchResult(TENANT_ID, METRIC_NAME, null);
        Assert.assertTrue("result1 should equal self", result1.equals(result1));
        Assert.assertTrue("result1 should equal result2", result1.equals(result2));
        Assert.assertTrue("resultEnums1 should not equal null", !result1.equals(null));

        String METRIC_NAME2 = "metric2";
        SearchResult result3 = new SearchResult(TENANT_ID, METRIC_NAME2, UNIT);
        SearchResult result4 = new SearchResult(TENANT_ID, METRIC_NAME2, UNIT);
        Assert.assertTrue("result3 should equal result4", result3.equals(result4));
        Assert.assertTrue("result1 should not equal result3", !result1.equals(result3));

        // with enum values
        ArrayList<String> enumValues1 = new ArrayList<String>();
        enumValues1.add("e1");
        enumValues1.add("e2");

        ArrayList<String> enumValues2 = new ArrayList<String>();
        enumValues2.add("e1");
        enumValues2.add("e2");

        SearchResult resultEnums1 = new SearchResult(TENANT_ID, METRIC_NAME, null, enumValues1);
        SearchResult resultEnums2 = new SearchResult(TENANT_ID, METRIC_NAME, null, enumValues2);
        Assert.assertTrue("resultEnums1 should equal resultEnums2", resultEnums1.equals(resultEnums2));
        Assert.assertTrue("resultEnums1 should not equal result1", !resultEnums1.equals(result1));
        Assert.assertTrue("result1 should not equal resultEnums1", !result1.equals(resultEnums1));

        ArrayList<String> enumValues3 = new ArrayList<String>();
        enumValues3.add("e1");
        enumValues3.add("d2");

        SearchResult resultEnums3 = new SearchResult(TENANT_ID, METRIC_NAME, null, enumValues3);
        SearchResult resultEnums4 = new SearchResult(TENANT_ID, METRIC_NAME, null, enumValues3);

        Assert.assertTrue("resultEnums3 should equal resultEnums4", resultEnums3.equals(resultEnums4));
        Assert.assertTrue("resultEnums3 should not equal resultEnums1", !resultEnums3.equals(resultEnums1));
        Assert.assertTrue("resultEnums3 should not equal resultEnums2", !resultEnums3.equals(resultEnums2));

        // assert equals for Object cast
        Assert.assertTrue("resultEnums1 should equal resultEnums2", resultEnums1.equals(((Object)resultEnums1)));
        Assert.assertTrue("resultEnums1 should not equal null", !resultEnums1.equals((Object)null));
        Assert.assertTrue("resultEnums1 should not equal another object class", !resultEnums1.equals((Object)1));
        Assert.assertTrue("resultEnums1 should equal resultEnums2 casted to Object", resultEnums1.equals(((Object)resultEnums2)));
    }
}
