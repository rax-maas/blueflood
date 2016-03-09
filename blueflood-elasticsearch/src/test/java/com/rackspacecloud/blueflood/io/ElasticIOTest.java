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
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ElasticIOTest extends BaseElasticTest {


    @Before
    public void setup() throws IOException {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        elasticIO = new ElasticIO(esSetup.client());

        elasticIO.insertDiscovery(createTestMetrics(TENANT_A));
        elasticIO.insertDiscovery(createTestMetrics(TENANT_B));
        elasticIO.insertDiscovery(createTestMetricsFromInterface(TENANT_C));

        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateSingleRequest_WithNullMetricName() throws IOException {
        Discovery discovery = new Discovery(TENANT_A, null);
        elasticIO.createSingleRequest(discovery);
    }

    @Test
    public void testCreateSingleRequest() throws IOException {
        final String METRIC_NAME = "a.b.c.m1";
        Discovery discovery = new Discovery(TENANT_A, METRIC_NAME);
        IndexRequestBuilder builder = elasticIO.createSingleRequest(discovery);
        Assert.assertNotNull(builder);
        Assert.assertEquals(TENANT_A + ":" + METRIC_NAME, builder.request().id());
        final String expectedIndex =
                "index {" +
                        "[" + ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE + "]" +
                        "[" + ElasticIO.ES_DOCUMENT_TYPE + "]" +
                        "["+ TENANT_A + ":" + METRIC_NAME + "], " +
                        "source[{" +
                        "\"tenantId\":\"" + TENANT_A + "\"," +
                        "\"metric_name\":\"" + METRIC_NAME + "\"" +
                        "}]}";
        Assert.assertEquals(expectedIndex, builder.request().toString());
        Assert.assertEquals(builder.request().routing(), TENANT_A);
    }

    @Test
    public void testNoCrossTenantResults() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "*");
        Assert.assertEquals(NUM_DOCS, results.size());
        for (SearchResult result : results) {
            Assert.assertNotNull(result.getTenantId());
            Assert.assertNotSame(TENANT_B, result.getTenantId());
        }
    }

    @Test
    public void testWildCard() throws Exception {
        testWildcard(TENANT_A, UNIT);
    }

    @Test
    public void testWildcardForPreaggregatedMetric() throws Exception {
        testWildcard(TENANT_C, null);
    }

    @Test
    public void testBatchQueryWithNoWildCards() throws Exception {
        String tenantId = TENANT_A;
        String query1 = "one.two.three00.fourA.five1";
        String query2 = "one.two.three01.fourA.five2";
        List<SearchResult> results;
        ArrayList<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        results = elasticIO.search(tenantId, queries);
        Assert.assertEquals(results.size(), 2); //we searched for 2 unique metrics
        results.contains(new SearchResult(TENANT_A, query1, UNIT));
        results.contains(new SearchResult(TENANT_A, query2, UNIT));
    }

    @Test
    public void testBatchQueryWithWildCards() throws Exception {
        String tenantId = TENANT_A;
        String query1 = "one.two.three00.fourA.*";
        String query2 = "one.two.*.fourA.five2";
        List<SearchResult> results;
        ArrayList<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        results = elasticIO.search(tenantId, queries);
        // query1 will return 3 results, query2 will return 30 results, but we get back 32 because of intersection
        Assert.assertEquals(results.size(), 32);
    }

    @Test
    public void testBatchQueryWithWildCards2() throws Exception {
        String tenantId = TENANT_A;
        String query1 = "*.two.three00.fourA.five1";
        String query2 = "*.two.three01.fourA.five2";
        List<SearchResult> results;
        ArrayList<String> queries = new ArrayList<String>();
        queries.add(query1);
        queries.add(query2);
        results = elasticIO.search(tenantId, queries);
        Assert.assertEquals(results.size(), 2);
    }

    public void testWildcard(String tenantId, String unit) throws Exception {
        SearchResult entry;
        List<SearchResult> results;
        results = elasticIO.search(tenantId, "one.two.*");
        List<Locator> locators = locatorMap.get(tenantId);
        Assert.assertEquals(locators.size(), results.size());
        for (Locator locator : locators) {
            entry =  new SearchResult(tenantId, locator.getMetricName(), unit);
            Assert.assertTrue((results.contains(entry)));
        }

        results = elasticIO.search(tenantId, "*.fourA.*");
        Assert.assertEquals(NUM_PARENT_ELEMENTS * NUM_GRANDCHILD_ELEMENTS, results.size());
        for (int x = 0; x < NUM_PARENT_ELEMENTS; x++) {
            for (int z = 0; z < NUM_GRANDCHILD_ELEMENTS; z++) {
                entry = createExpectedResult(tenantId, x, "A", z, unit);
                Assert.assertTrue(results.contains(entry));
            }
        }

        results = elasticIO.search(tenantId, "*.three1*.four*.five2");
        Assert.assertEquals(10 * CHILD_ELEMENTS.size(), results.size());
        for (int x = 10; x < 20; x++) {
            for (String y : CHILD_ELEMENTS) {
                entry = createExpectedResult(tenantId, x, y, 2, unit);
                Assert.assertTrue(results.contains(entry));
            }
        }
    }

    @Test
    public void testGlobMatching() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "one.two.{three00,three01}.fourA.five0");
        Assert.assertEquals(results.size(), 2);
        results.contains(new SearchResult(TENANT_A, "one.two.three00.fourA.five0", UNIT));
        results.contains(new SearchResult(TENANT_A, "one.two.three01.fourA.five0", UNIT));
    }

    @Test
    public void testGlobMatching2() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "one.two.three0?.fourA.five0");
        List<SearchResult> results2 = elasticIO.search(TENANT_A, "one.two.three0[0-9].fourA.five0");
        Assert.assertEquals(10, results.size());
        for (SearchResult result : results) {
            Assert.assertTrue(result.getMetricName().startsWith("one.two.three"));
            Assert.assertEquals(result.getTenantId(), TENANT_A);
            results2.contains(result);
        }
    }

    @Test
    public void testGlobMatching3() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "one.two.three0[01].fourA.five0");
        Assert.assertEquals(2, results.size());
        for (SearchResult result : results) {
            Assert.assertTrue(result.getMetricName().equals("one.two.three00.fourA.five0") || result.getMetricName().equals("one.two.three01.fourA.five0"));
        }
    }

    @Test
    public void testDeDupMetrics() throws Exception {
        // New index name and the locator to be written to it
        String ES_DUP = ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE + "_2";
        Locator testLocator = createTestLocator(TENANT_A, 0, "A", 0);
        // Metric is aleady there in old
        List<SearchResult> results = elasticIO.search(TENANT_A, testLocator.getMetricName());
        Assert.assertEquals(results.size(), 1);
        Assert.assertEquals(results.get(0).getMetricName(), testLocator.getMetricName());
        // Actually create the new index
        esSetup.execute(EsSetup.createIndex(ES_DUP)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        // Insert metric into the new index
        elasticIO.setINDEX_NAME_WRITE(ES_DUP);
        ArrayList metricList = new ArrayList();
        metricList.add(new Metric(createTestLocator(TENANT_A, 0, "A", 0), "blarg", 0, new TimeValue(1, TimeUnit.DAYS), UNIT));
        elasticIO.insertDiscovery(metricList);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
        // Set up aliases
        esSetup.client().admin().indices().prepareAliases().addAlias(ES_DUP, "metric_metadata_read")
                .addAlias(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE, "metric_metadata_read").execute().actionGet();
        elasticIO.setINDEX_NAME_READ("metric_metadata_read");
        results = elasticIO.search(TENANT_A, testLocator.getMetricName());
        // Should just be one result
        Assert.assertEquals(results.size(), 1);
        Assert.assertEquals(results.get(0).getMetricName(), testLocator.getMetricName());
        elasticIO.setINDEX_NAME_READ(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ.getDefaultValue());
        elasticIO.setINDEX_NAME_WRITE(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE.getDefaultValue());
    }


    @Test
    public void testGetMetricTokens() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Assert.assertEquals("Invalid total number of results", 1, results.size());
        Assert.assertEquals("Next token mismatch", "one", results.get(0).getPath());
        Assert.assertEquals("isLeaf for token", false, results.get(0).isLeaf());
    }

    @Test
    public void testGetMetricTokensMultipleMetrics() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one|false");
            add("foo|false");
        }};

        Assert.assertEquals("Invalid total number of results", 2, results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testGetMetricTokensSingleLevelPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.*";

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Assert.assertEquals("Invalid total number of results", 1, results.size());
        Assert.assertEquals("Next token mismatch", "one.two", results.get(0).getPath());
        Assert.assertEquals("Next level indicator wrong for token", false, results.get(0).isLeaf());
    }

    @Test
    public void testGetMetricTokensWithWildCardPrefixMultipleLevels() throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two|false");
            add("foo.bar|false");
        }};

        Assert.assertEquals("Invalid total number of results", 2, results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testGetMetricTokensWithMultiLevelPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.*";

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Assert.assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricToken metricToken : results) {
            Assert.assertFalse("isLeaf value", metricToken.isLeaf());
        }
    }

    @Test
    public void testGetMetricTokensWithWildCardPrefixAtTheEnd() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.three*.*";

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Assert.assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS * CHILD_ELEMENTS.size(), results.size());
        for (MetricToken metricToken : results) {
            Assert.assertFalse("isLeaf value", metricToken.isLeaf());;
        }
    }

    @Test
    public void testGetMetricTokensWithWildCardAndBracketsPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.{two,foo}.[ta]hree00.*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two.three00.fourA|false");
            add("one.two.three00.fourB|false");
            add("one.two.three00.fourC|false");
            add("one.foo.three00.bar|false");
        }};

        Assert.assertEquals("Invalid total number of results", CHILD_ELEMENTS.size() + 1, results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testGetMetricTokensWithMultiWildCardPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*.*";

        List<MetricToken> results = elasticIO.getMetricTokens(tenantId, query);

        Assert.assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricToken metricToken : results) {
            Assert.assertFalse("isLeaf value", metricToken.isLeaf());;
        }
    }


}
