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

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.EnumMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EnumElasticIOTest {

    private EnumElasticIO enumElasticIO;
    private EsSetup esSetup;

    private static final String TENANT_1 = "100000";
    private static final String TENANT_2 = "200000";

    private static final String METRIC_NAME_A1 = "a.m1";
    private static final String METRIC_NAME_A2 = "a.m2";

    private static final String METRIC_NAME_B1 = "b.m1";
    private static final String METRIC_NAME_B2 = "b.m2";
    private static final String METRIC_NAME_B3 = "b.m3";

    ArrayList<String> enumValues = new ArrayList<String>();

    @Before
    public void setup() throws IOException {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping(EnumElasticIO.ENUMS_DOCUMENT_TYPE, EsSetup.fromClassPath("metrics_mapping_enums.json")));

        enumElasticIO = new EnumElasticIO(esSetup.client());
        enumElasticIO.setINDEX_NAME_READ(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);
        enumElasticIO.setINDEX_NAME_WRITE(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);

        createTestMetrics();
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    private IMetric createTestMetric(String tenantId, String metricName, ArrayList<String> enumValues) {
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        return new EnumMetric(locator, "", 0, new TimeValue(1, TimeUnit.DAYS), enumValues);
    }

    private void createTestMetrics() throws IOException {

        enumValues.add("v1");
        enumValues.add("v2");
        enumValues.add("v3");

        List<IMetric> metrics = new ArrayList<IMetric>();

        // add TENANT_1 metrics
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_A1, enumValues));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_A2, enumValues));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_B1, enumValues));
        metrics.add(createTestMetric(TENANT_1, METRIC_NAME_B2, enumValues));

        // add TENANT_2 metrics
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B1, enumValues));
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B2, enumValues));
        metrics.add(createTestMetric(TENANT_2, METRIC_NAME_B3, enumValues));
        enumElasticIO.insertDiscovery(metrics);

        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateSingleRequest_WithNullMetricName() throws IOException {
        Discovery discovery = new Discovery(TENANT_1, null);
        enumElasticIO.createSingleRequest(discovery);
    }

    @Test
    public void testCreateSingleRequest() throws IOException {
        Discovery discovery = new Discovery(TENANT_1, METRIC_NAME_A1);
        IndexRequestBuilder builder = enumElasticIO.createSingleRequest(discovery);
        Assert.assertNotNull(builder);
        Assert.assertEquals(TENANT_1 + ":" + METRIC_NAME_A1, builder.request().id());
        final String expectedIndex =
                "index {" +
                    "[" + EnumElasticIO.ENUMS_INDEX_NAME_WRITE + "]" +
                    "[" + EnumElasticIO.ENUMS_DOCUMENT_TYPE + "]" +
                    "["+ TENANT_1 + ":" + METRIC_NAME_A1 + "], " +
                    "source[{" +
                        "\"tenantId\":\"" + TENANT_1 + "\"," +
                        "\"metric_name\":\"" + METRIC_NAME_A1 + "\"" +
                    "}]}";
        Assert.assertEquals(expectedIndex, builder.request().toString());
        Assert.assertEquals(builder.request().routing(), TENANT_1);
    }

    @Test
    public void testSearchWithNoWildCard() throws Exception {
        List<SearchResult> results = enumElasticIO.search(TENANT_1, METRIC_NAME_A1);
        Assert.assertEquals("results should have 1 result", 1, results.size());

        // get the result
        SearchResult result = results.get(0);
        Assert.assertEquals(TENANT_1, result.getTenantId());
        Assert.assertEquals(METRIC_NAME_A1, result.getMetricName());
        Assert.assertNotNull(result.getEnumValues());
        Assert.assertTrue(result.getEnumValues().equals(enumValues));
    }

    @Test
    public void testSearchWithWildCard() throws Exception {
        // tenant1 expected search results
        SearchResult tenant1Entry1 =  new SearchResult(TENANT_1, METRIC_NAME_A1, null, enumValues);
        SearchResult tenant1Entry2 =  new SearchResult(TENANT_1, METRIC_NAME_A2, null, enumValues);
        SearchResult tenant1Entry3 =  new SearchResult(TENANT_1, METRIC_NAME_B1, null, enumValues);
        SearchResult tenant1Entry4 =  new SearchResult(TENANT_1, METRIC_NAME_B2, null, enumValues);

        // tenant2 expected search results
        SearchResult tenant2Entry1 =  new SearchResult(TENANT_2, METRIC_NAME_B1, null, enumValues);
        SearchResult tenant2Entry2 =  new SearchResult(TENANT_2, METRIC_NAME_B2, null, enumValues);
        SearchResult tenant2Entry3 =  new SearchResult(TENANT_2, METRIC_NAME_B3, null, enumValues);

        // results1 = search all tenant1
        List<SearchResult> results1 = enumElasticIO.search(TENANT_1, "*");
        Assert.assertEquals("results1 should have 4 results", 4, results1.size());

        // results2 = search all tenant2
        List<SearchResult> results2 = enumElasticIO.search(TENANT_2, "*");
        Assert.assertEquals("results2 should have 3 results", 3, results2.size());

        // assert results1
        Assert.assertTrue("results1 should contain tenant1 entry1", results1.contains(tenant1Entry1));
        Assert.assertTrue("results1 should contain tenant1 entry2", results1.contains(tenant1Entry2));
        Assert.assertTrue("results1 should contain tenant1 entry3", results1.contains(tenant1Entry3));
        Assert.assertTrue("results1 should contain tenant1 entry4", results1.contains(tenant1Entry4));
        Assert.assertTrue("results1 should not contain tenant2 entry1", !results1.contains(tenant2Entry1));
        Assert.assertTrue("results1 should not contain tenant2 entry2", !results1.contains(tenant2Entry2));
        Assert.assertTrue("results1 should not contain tenant2 entry3", !results1.contains(tenant2Entry3));

        // assert results2
        Assert.assertTrue("results2 should contain tenant2 entry1", results2.contains(tenant2Entry1));
        Assert.assertTrue("results2 should contain tenant2 entry2", results2.contains(tenant2Entry2));
        Assert.assertTrue("results2 should contain tenant2 entry3", results2.contains(tenant2Entry3));
        Assert.assertTrue("results1 should not contain tenant1 entry1", !results2.contains(tenant1Entry1));
        Assert.assertTrue("results1 should not contain tenant1 entry2", !results2.contains(tenant1Entry2));

        // results3 = search *.m1 for tenant1
        List<SearchResult> results3 = enumElasticIO.search(TENANT_1, "*.m1");
        Assert.assertEquals("results3 should have 2 results", 2, results3.size());
        Assert.assertTrue("results3 should contain tenant1 entry1", results3.contains(tenant1Entry1));
        Assert.assertTrue("results3 should contain tenant1 entry3", results3.contains(tenant1Entry3));
        Assert.assertTrue("results3 should not contain tenant1 entry2", !results3.contains(tenant1Entry2));
        Assert.assertTrue("results3 should not contain tenant1 entry4", !results3.contains(tenant1Entry4));
        Assert.assertTrue("results3 should not contain tenant2 entry1", !results3.contains(tenant2Entry1));
        Assert.assertTrue("results3 should not contain tenant2 entry2", !results3.contains(tenant2Entry2));
        Assert.assertTrue("results3 should not contain tenant2 entry3", !results3.contains(tenant2Entry3));
    }

    @Test
    public void testBatchQueryWithWildCards() throws Exception {
        String tenantId = TENANT_1;
        String query1 = "a.*";
        String query2 = "*.m1";
        List<SearchResult> results;
        ArrayList<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        results = enumElasticIO.search(tenantId, queries);

        // query1 will return 2 results, query2 will return 2 results, but we get back 3 because of intersection
        Assert.assertEquals("results should have 3 results", results.size(), 3);

        SearchResult result1 =  new SearchResult(TENANT_1, METRIC_NAME_A1, null, enumValues);
        SearchResult result2 =  new SearchResult(TENANT_1, METRIC_NAME_A2, null, enumValues);
        SearchResult result3 =  new SearchResult(TENANT_1, METRIC_NAME_B1, null, enumValues);

        Assert.assertTrue("results should contain result1", results.contains(result1));
        Assert.assertTrue("results should contain result2", results.contains(result2));
        Assert.assertTrue("results should contain result3", results.contains(result3));
    }
}
