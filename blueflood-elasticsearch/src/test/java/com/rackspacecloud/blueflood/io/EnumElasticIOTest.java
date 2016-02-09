/*
 * Copyright 2015 Rackspace
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
import com.google.common.collect.Iterables;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnumElasticIOTest extends BaseElasticTest {

    private EnumElasticIO enumElasticIO;

    private Client esClientMock = mock(Client.class);
    private BulkRequestBuilder bulkRequestBuilderMock = mock(BulkRequestBuilder.class);
    private ListenableActionFuture listenableActionFutureMock = mock(ListenableActionFuture.class);
    private IndexRequestBuilder indexRequestBuilderMock = mock(IndexRequestBuilder.class);

    private static final String TENANT_1 = "100000";
    private static final String TENANT_2 = "200000";

    private static final String METRIC_NAME_A1 = "a.m1";
    private static final String METRIC_NAME_A2 = "a.m2";

    private static final String METRIC_NAME_B1 = "b.m1";
    private static final String METRIC_NAME_B2 = "b.m2";
    private static final String METRIC_NAME_B3 = "b.m3";

    long collectionTime = 0;
    TimeValue ttl = new TimeValue(1, TimeUnit.DAYS);

    BluefloodEnumRollup rollupWithEnumValues = new BluefloodEnumRollup()
            .withEnumValue("v1", 1L)
            .withEnumValue("v2", 1L)
            .withEnumValue("v3", 1L);
    ArrayList<String> enumValues = rollupWithEnumValues.getStringEnumValues();

    @Before
    public void setup() throws IOException {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        esSetup.execute(EsSetup.createIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping(EnumElasticIO.ENUMS_DOCUMENT_TYPE, EsSetup.fromClassPath("metrics_mapping_enums.json")));
        elasticIO = new ElasticIO(esSetup.client());

        enumElasticIO = new EnumElasticIO(esSetup.client());
        enumElasticIO.setINDEX_NAME_READ(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);
        enumElasticIO.setINDEX_NAME_WRITE(EnumElasticIO.ENUMS_INDEX_NAME_WRITE);

        // setup mock for ElasticSearch Client
        when(bulkRequestBuilderMock.execute()).thenReturn(listenableActionFutureMock);
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        when(esClientMock.prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE))
                .thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setId(anyString())).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setSource(any(XContentBuilder.class))).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setCreate(anyBoolean())).thenReturn(indexRequestBuilderMock);
        when(indexRequestBuilderMock.setRouting(anyString())).thenReturn(indexRequestBuilderMock);
    }

    @After
    public void tearDown() {
        esSetup.terminate();
    }

    @Test
    public void testCreateEnumElasticIO() {
        EnumElasticIO enumElasticIO1 = new EnumElasticIO();
        Assert.assertNotNull("create EnumElasticIO() failed", enumElasticIO1);

        EnumElasticIO enumElasticIO2 = new EnumElasticIO(esClientMock);
        Assert.assertNotNull("create EnumElasticIO(client) failed", enumElasticIO2);

        ElasticClientManager managerMock = mock(ElasticClientManager.class);
        EnumElasticIO enumElasticIO3 = new EnumElasticIO(managerMock);
        Assert.assertNotNull("create EnumElasticIO(manager) failed", enumElasticIO3);
        verify(managerMock, times(1)).getClient();
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
    public void testInsertDiscoveryWithEmptyBatch() throws IOException {
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        enumElasticIO.setClient(esClientMock);

        // create empty metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        enumElasticIO.insertDiscovery(metrics);

        // verify nothing is executed
        verify(esClientMock, never()).prepareBulk();
        verify(bulkRequestBuilderMock, never()).execute();
    }

    @Test
    public void testInsertDiscoveryWithInvalidMetricClass() throws IOException {
        ListenableActionFuture listenableActionFutureMock = mock(ListenableActionFuture.class);
        when(esClientMock.prepareBulk()).thenReturn(bulkRequestBuilderMock);
        when(bulkRequestBuilderMock.execute()).thenReturn(listenableActionFutureMock);
        enumElasticIO.setClient(esClientMock);

        // create empty metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        metrics.add(new Metric(locator, "", 0L, new TimeValue(1, TimeUnit.DAYS), null));
        enumElasticIO.insertDiscovery(metrics);

        // verify
        verify(bulkRequestBuilderMock, never()).add(any(IndexRequestBuilder.class));
    }

    @Test
    public void testInsertDiscoverySingle() throws IOException {
        enumElasticIO.setClient(esClientMock);

        // test single metric insert
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        IMetric metric = new PreaggregatedMetric(collectionTime, locator, ttl, rollupWithEnumValues);
        enumElasticIO.insertDiscovery(metric);
        // verify
        verify(esClientMock, times(1)).prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE);
        verify(indexRequestBuilderMock).setId(TENANT_1 + ":" + METRIC_NAME_A1);
        verify(bulkRequestBuilderMock, times(1)).execute();
    }

    @Test
    public void testInsertDiscoveryBatch() throws IOException {
        enumElasticIO.setClient(esClientMock);

        // create PreaggregatedMetric metrics list and call insertDiscovery with it
        List<IMetric> metrics = new ArrayList<IMetric>();
        Locator locator1 = Locator.createLocatorFromPathComponents(TENANT_1, METRIC_NAME_A1);
        Locator locator2 = Locator.createLocatorFromPathComponents(TENANT_2, METRIC_NAME_B1);
        metrics.add(new PreaggregatedMetric(collectionTime, locator1, ttl, rollupWithEnumValues));
        metrics.add(new PreaggregatedMetric(collectionTime, locator2, ttl, rollupWithEnumValues));
        enumElasticIO.insertDiscovery(metrics);

        // verify client execute with right params
        verify(esClientMock, times(2)).prepareIndex(EnumElasticIO.ENUMS_INDEX_NAME_WRITE, EnumElasticIO.ENUMS_DOCUMENT_TYPE);
        verify(indexRequestBuilderMock, times(2)).setId(anyString());
        verify(indexRequestBuilderMock).setId(TENANT_1 + ":" + METRIC_NAME_A1);
        verify(indexRequestBuilderMock).setId(TENANT_2 + ":" + METRIC_NAME_B1);
        verify(indexRequestBuilderMock, times(2)).setSource(any(XContentBuilder.class));
        verify(indexRequestBuilderMock, times(2)).setRouting(anyString());
        verify(indexRequestBuilderMock).setRouting(TENANT_1);
        verify(indexRequestBuilderMock).setRouting(TENANT_2);
        verify(bulkRequestBuilderMock, times(1)).execute();
    }

    @Test
    public void testSearchWithNoWildCard() throws Exception {

        createEnumTestMetrics();

        List<SearchResult> results = enumElasticIO.search(TENANT_1, METRIC_NAME_A1);
        Assert.assertEquals("results should have 1 result", 1, results.size());

        // get the result
        SearchResult result = results.get(0);
        Assert.assertEquals(TENANT_1, result.getTenantId());
        Assert.assertEquals(METRIC_NAME_A1, result.getMetricName());
        Assert.assertNotNull(result.getEnumValues());
        assertTrue(result.getEnumValues().equals(enumValues));
    }

    @Test
    public void testSearchWithWildCard() throws Exception {

        createEnumTestMetrics();

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
        assertTrue("results1 should contain tenant1 entry1", results1.contains(tenant1Entry1));
        assertTrue("results1 should contain tenant1 entry2", results1.contains(tenant1Entry2));
        assertTrue("results1 should contain tenant1 entry3", results1.contains(tenant1Entry3));
        assertTrue("results1 should contain tenant1 entry4", results1.contains(tenant1Entry4));
        assertTrue("results1 should not contain tenant2 entry1", !results1.contains(tenant2Entry1));
        assertTrue("results1 should not contain tenant2 entry2", !results1.contains(tenant2Entry2));
        assertTrue("results1 should not contain tenant2 entry3", !results1.contains(tenant2Entry3));

        // assert results2
        assertTrue("results2 should contain tenant2 entry1", results2.contains(tenant2Entry1));
        assertTrue("results2 should contain tenant2 entry2", results2.contains(tenant2Entry2));
        assertTrue("results2 should contain tenant2 entry3", results2.contains(tenant2Entry3));
        assertTrue("results2 should not contain tenant1 entry1", !results2.contains(tenant1Entry1));
        assertTrue("results2 should not contain tenant1 entry2", !results2.contains(tenant1Entry2));

        // results3 = search *.m1 for tenant1
        List<SearchResult> results3 = enumElasticIO.search(TENANT_1, "*.m1");
        Assert.assertEquals("results3 should have 2 results", 2, results3.size());
        assertTrue("results3 should contain tenant1 entry1", results3.contains(tenant1Entry1));
        assertTrue("results3 should contain tenant1 entry3", results3.contains(tenant1Entry3));
        assertTrue("results3 should not contain tenant1 entry2", !results3.contains(tenant1Entry2));
        assertTrue("results3 should not contain tenant1 entry4", !results3.contains(tenant1Entry4));
        assertTrue("results3 should not contain tenant2 entry1", !results3.contains(tenant2Entry1));
        assertTrue("results3 should not contain tenant2 entry2", !results3.contains(tenant2Entry2));
        assertTrue("results3 should not contain tenant2 entry3", !results3.contains(tenant2Entry3));
    }

    @Test
    public void testBatchQueryWithWildCards() throws Exception {

        createEnumTestMetrics();

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

        assertTrue("results should contain result1", results.contains(result1));
        assertTrue("results should contain result2", results.contains(result2));
        assertTrue("results should contain result3", results.contains(result3));
    }

    @Test
    public void testGetNextTokenForEnumsMultiTenant() throws Exception {

        createEnumTestMetrics();

        String tenantId = TENANT_1;
        String prefix = "a";

        List<TokenInfo> resultsTenant1 = enumElasticIO.getNextTokens(tenantId, prefix);
        Set<String> expectedResultsTenant1 = new HashSet<String>() {{
            add("m1|true");
            add("m2|true");
        }};

        verifyTokenAndNextLevelFlag(resultsTenant1, expectedResultsTenant1);

        tenantId = TENANT_2;
        prefix = "b";

        List<TokenInfo> resultsTenant2 = enumElasticIO.getNextTokens(tenantId, prefix);
        Set<String> expectedResultsTenant2 = new HashSet<String>() {{
            add("m1|true");
            add("m2|true");
            add("m3|true");
        }};

        verifyTokenAndNextLevelFlag(resultsTenant2, expectedResultsTenant2);
    }

    @Test
    public void testRegexForTokensForAPrefix() {
        String regex = enumElasticIO.regexForNextNLevels("foo.bar", 2);

        Set<String> metricIndexesToBeMatched = new HashSet<String>() {{
            add("foo.bar");
            add("foo.bar.baz");
            add("foo.bar.baz.qux");
        }};

        Set<String> metricIndexesThatShouldNotMatch = new HashSet<String>() {{
            add("foo");
            add("foo.bar.baz.qux.x");
            add("moo.bar.baz.qux");
            add("moo.bar.baz");
        }};

        Set<String> matchedMetricIndexes = new HashSet<String>();
        Pattern pattern = Pattern.compile(regex);
        for (String metricIndex: Iterables.concat(metricIndexesThatShouldNotMatch, metricIndexesToBeMatched)) {

            Matcher matcher = pattern.matcher(metricIndex);
            if (matcher.matches()) {
                matchedMetricIndexes.add(metricIndex);
            }
        }

        assertEquals("matched indexes size", metricIndexesToBeMatched.size(), matchedMetricIndexes.size());
        assertTrue("All matched metric indexes should be valid", metricIndexesToBeMatched.containsAll(matchedMetricIndexes));
        assertTrue("matched metric indexes should not be more than expected", matchedMetricIndexes.containsAll(metricIndexesToBeMatched));
        assertTrue("matched indexes should not contain these indexes",
                Collections.disjoint(metricIndexesToBeMatched, metricIndexesThatShouldNotMatch));
    }

    @Test
    public void testRegexForTokensForAPrefixWithWildCard() {
        String regex = enumElasticIO.regexForNextNLevels("foo.bar.*", 2);

        Set<String> metricIndexesToBeMatched = new HashSet<String>() {{
            add("foo.bar.baz");
            add("foo.bar.baz.qux");
            add("foo.bar.baz.qux.x");
        }};

        Set<String> metricIndexesThatShouldNotMatch = new HashSet<String>() {{
            add("foo");
            add("foo.bar");
            add("foo.bar.baz.qux.x.y");
            add("moo.bar.baz");
        }};

        Set<String> matchedMetricIndexes = new HashSet<String>();
        Pattern pattern = Pattern.compile(regex);
        for (String metricIndex: Iterables.concat(metricIndexesThatShouldNotMatch, metricIndexesToBeMatched)) {

            Matcher matcher = pattern.matcher(metricIndex);
            if (matcher.matches()) {
                matchedMetricIndexes.add(metricIndex);
            }
        }

        assertEquals("matched indexes size", metricIndexesToBeMatched.size(), matchedMetricIndexes.size());
        assertTrue("All matched metric indexes should be valid", metricIndexesToBeMatched.containsAll(matchedMetricIndexes));
        assertTrue("matched metric indexes should not be more than expected", matchedMetricIndexes.containsAll(metricIndexesToBeMatched));
        assertTrue("matched indexes should not contain these indexes",
                Collections.disjoint(metricIndexesToBeMatched, metricIndexesThatShouldNotMatch));
    }

    @Test
    public void testRegexForTokensForASingleLevelPrefix() {
        String regex = enumElasticIO.regexForNextNLevels("foo", 2);

        Set<String> metricIndexesToBeMatched = new HashSet<String>() {{
            add("foo.bar");
            add("foo.bar.baz");
        }};

        Set<String> metricIndexesThatShouldNotMatch = new HashSet<String>() {{
            add("foo");
            add("test");
            add("foo.bar.baz.qux");
        }};

        Set<String> matchedMetricIndexes = new HashSet<String>();
        Pattern pattern = Pattern.compile(regex);
        for (String metricIndex: Iterables.concat(metricIndexesThatShouldNotMatch, metricIndexesToBeMatched)) {

            Matcher matcher = pattern.matcher(metricIndex);
            if (matcher.matches()) {
                matchedMetricIndexes.add(metricIndex);
            }
        }

        assertEquals("matched indexes size", metricIndexesToBeMatched.size(), matchedMetricIndexes.size());
        assertTrue("All matched metric indexes should be valid", metricIndexesToBeMatched.containsAll(matchedMetricIndexes));
        assertTrue("matched metric indexes should not be more than expected", matchedMetricIndexes.containsAll(metricIndexesToBeMatched));
        assertTrue("matched indexes should not contain these indexes",
                Collections.disjoint(metricIndexesToBeMatched, metricIndexesThatShouldNotMatch));
    }

    @Test
    public void testRegexForTokensForASingleLevelWildCardPrefix() {
        String regex = enumElasticIO.regexForNextNLevels("*", 2);

        Set<String> metricIndexesToBeMatched = new HashSet<String>() {{
            add("foo.bar");
            add("foo.bar.baz");
            add("x.y.z");
            add("a.b");
        }};

        Set<String> metricIndexesThatShouldNotMatch = new HashSet<String>() {{
            add("foo");
            add("x");
            add("test");
            add("foo.bar.baz.qux");
        }};

        Set<String> matchedMetricIndexes = new HashSet<String>();
        Pattern pattern = Pattern.compile(regex);
        for (String metricIndex: Iterables.concat(metricIndexesThatShouldNotMatch, metricIndexesToBeMatched)) {

            Matcher matcher = pattern.matcher(metricIndex);
            if (matcher.matches()) {
                matchedMetricIndexes.add(metricIndex);
            }
        }

        assertEquals("matched indexes size", metricIndexesToBeMatched.size(), matchedMetricIndexes.size());
        assertTrue("All matched metric indexes should be valid", metricIndexesToBeMatched.containsAll(matchedMetricIndexes));
        assertTrue("matched metric indexes should not be more than expected", matchedMetricIndexes.containsAll(metricIndexesToBeMatched));
        assertTrue("matched indexes should not contain these indexes",
                Collections.disjoint(metricIndexesToBeMatched, metricIndexesThatShouldNotMatch));
    }

    @Test
    public void testRegexForTokensForEmptyPrefix() {
        String regex = enumElasticIO.regexForNextNLevels("", 2);

        Set<String> metricIndexesToBeMatched = new HashSet<String>() {{
            add("foo.bar");
            add("a.b");
        }};

        Set<String> metricIndexesThatShouldNotMatch = new HashSet<String>() {{
            add("foo");
            add("x");
            add("test");
            add("foo.bar.baz");
            add("x.y.z");
            add("foo.bar.baz.qux");
        }};

        Set<String> matchedMetricIndexes = new HashSet<String>();
        Pattern pattern = Pattern.compile(regex);
        for (String metricIndex: Iterables.concat(metricIndexesThatShouldNotMatch, metricIndexesToBeMatched)) {

            Matcher matcher = pattern.matcher(metricIndex);
            if (matcher.matches()) {
                matchedMetricIndexes.add(metricIndex);
            }
        }

        assertEquals("matched indexes size", metricIndexesToBeMatched.size(), matchedMetricIndexes.size());
        assertTrue("All matched metric indexes should be valid", metricIndexesToBeMatched.containsAll(matchedMetricIndexes));
        assertTrue("matched metric indexes should not be more than expected", matchedMetricIndexes.containsAll(metricIndexesToBeMatched));
        assertTrue("matched indexes should not contain these indexes",
                Collections.disjoint(metricIndexesToBeMatched, metricIndexesThatShouldNotMatch));
    }

    @Test
    public void testGetNextTokenForEnumsWithoutPrefix() throws Exception {

        createEnumTestMetrics();

        String tenantId = TENANT_1;
        String prefix = "";

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);
        Set<String> expectedResults = new HashSet<String>() {{
            add("a|true");
            add("b|true");
        }};

        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testGetNextTokenForEnumsWithPrefix() throws Exception {

        createEnumTestMetrics();

        String tenantId = TENANT_1;
        String prefix = "a.m1";

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);
        Set<String> expectedResults = new HashSet<String>() {{
            add("v1|false");
            add("v2|false");
            add("v3|false");
        }};

        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testGetNextTokenWithCompleteMetricName() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        //complete metric name
        final String prefix = "one.two.three00.fourA.five0";


        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);
        Assert.assertEquals("Invalid total number of results", 0, results.size());
    }

    @Test
    public void testGetNextTokenWithNonExistentMetricName() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        //non-existent metric name
        final String prefix = "xxx.yyy.zzz";


        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);
        Assert.assertEquals("Invalid total number of results", 0, results.size());
    }

    @Test
    public void testWithEnumMetricAtNextLevel() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        //prefix as one level behind complete metric name. In this case one.two.three00.fourA.five[0-2]
        final String prefix = "one.two.three00.fourA";

        //enum metric at the next level of prefix
        final String enumMetricName = "one.two.three00.fourA.five100";
        createSingleEnumTestMetric(tenantId, enumMetricName);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        // since we picked the prefix as one level below a complete metric name, tokens returned will have
        // next level only if the complete metric name is an enum.
        Set<String> expectedResults = new HashSet<String>() {{
            add("five0|false");
            add("five1|false");
            add("five2|false");
            add("five100|true");
        }};

        verifyTokenAndNextLevelFlag(results, expectedResults);

        final String prefix1 = "one.two.three00.fourA.five100";
        List<TokenInfo> results1 = enumElasticIO.getNextTokens(tenantId, prefix1);

        Set<String> expectedResults1 = new HashSet<String>() {{
            add("ev1|false");
            add("ev2|false");

        }};

        verifyTokenAndNextLevelFlag(results1, expectedResults1);
    }

    @Test
    public void testWithEnumMetricAtNextLevelWithWildCardPrefix() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        final String prefix = "*";

        //enum metric at the next level of prefix
        final String enumMetricName = "foo.bar";

        createSingleEnumTestMetric(tenantId, enumMetricName);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        //since we picked the prefix as one same level as an enum metric, it should grab the enum
        //values of the metric as next level of tokens
        Set<String> expectedResults = new HashSet<String>() {{
            add("two|true");
            add("bar|true");
        }};

        Assert.assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }


    @Test
    public void testWithEnumMetricAtSameLevelWithWildCard() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        // prefix at the same level of an enum metric. Also prefix has regular metrics as next level
        // which in this case would be one.two.three00.fourA.five[0-2]
        final String prefix = "one.two.three00.four*";

        //enum metric at the same level of prefix
        final String enumMetricName = "one.two.three00.fourD";

        createSingleEnumTestMetric(tenantId, enumMetricName);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        //since we picked the prefix as one same level as an enum metric, it should grab the enum
        //values of the metric as next level of tokens
        Set<String> expectedResults = new HashSet<String>() {{
            add("five0|false");
            add("five1|false");
            add("five2|false");
            add("ev1|false");
            add("ev2|false");
        }};

        Assert.assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }



    @Test
    public void testWithEnumMetricAndRegularMetricAtSameLevel() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        // prefix at same level of complete metric name and also a complete enum metric name.
        final String prefix = "one.two.three*.four*.five*";

        //enum metric at the same level of prefix
        final String enumMetricName = "one.two.three00.fourA.five100";
        createSingleEnumTestMetric(tenantId, enumMetricName);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        //for this prefix, the only next level is the enum values of enum metric
        Set<String> expectedResults = new HashSet<String>() {{
            add("ev1|false");
            add("ev2|false");
        }};

        Assert.assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testWithEnumMetricAtSameLevelAndNextLevel() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        // prefix has enum metrics at the same level and next level. Also prefix has regular metrics as next level
        // which in this case would be one.two.three00.fourA.five[0-2]
        final String prefix = "one.two.three00.four*";

        //enum metric at the same level of prefix
        final String enumMetricName1 = "one.two.three00.fourD";

        //enum metric at next level of prefix
        final String enumMetricName2 = "one.two.three00.fourD.five100";

        Map<String, List<String>> enumData = new HashMap<String, List<String>>();

        enumData.put(enumMetricName1, new ArrayList<String>() {{
            add("ev1-1");
        }});

        enumData.put(enumMetricName2, new ArrayList<String>() {{
            add("ev2-1");
        }});

        createEnumTestMetrics(tenantId, enumData);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        //since we picked the prefix as one same level as an enum metric, it should grab the enum
        //values of the metric as next level of tokens
        Set<String> expectedResults = new HashSet<String>() {{
            add("five0|false");
            add("five1|false");
            add("five2|false");
            add("ev1-1|false");
            add("five100|true");
        }};

        Assert.assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testWithMetricAtNextLevel() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        //prefix as one level behind complete metric name. In this case one.two.three00.fourA.five[0-2]
        final String prefix = "one.two.three00";

        //complete metric name which is a part of existing metric name one.two.three00.fourA.five[0-2]
        final String metricName = "one.two.three00.fourA";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add(metricName);
        }});

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        // there is a complete metric name as next level of prefix.
        // there is also an incomplete metric name with the same same token as the complete one
        Set<String> expectedResults = new HashSet<String>() {{
            add("fourA|false");
            add("fourA|true");
            add("fourB|true");
            add("fourC|true");
        }};

        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    @Test
    public void testWithEnumMetricAtSameLevel() throws Exception {

        final String tenantId = TENANT_A;
        createTestMetrics(createTestMetrics(tenantId));

        final String prefix = "one.two.three00";

        //enum metric at same level of prefix
        final String enumMetricName = "one.two.three00";

        Map<String, List<String>> enumData = new HashMap<String, List<String>>();

        //enum values same as the next token of the given prefix
        enumData.put(enumMetricName, new ArrayList<String>() {{
            add("fourA");
            add("fourB");
            add("fourC");
        }});
        createEnumTestMetrics(tenantId, enumData);

        List<TokenInfo> results = enumElasticIO.getNextTokens(tenantId, prefix);

        // there is complete enum metric name at the same level as prefix
        // there are also metrics which have next levels for the given prefix.
        Set<String> expectedResults = new HashSet<String>() {{
            add("fourA|false");
            add("fourB|false");
            add("fourC|false");
            add("fourA|true");
            add("fourB|true");
            add("fourC|true");
        }};

        verifyTokenAndNextLevelFlag(results, expectedResults);
    }

    private IMetric createEnumTestMetric(String tenantId, String metricName) {
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        return new PreaggregatedMetric(0, locator, new TimeValue(1, TimeUnit.DAYS), rollupWithEnumValues);
    }

    private IMetric createEnumTestMetric(String tenantId, String metricName, BluefloodEnumRollup enumRollup) {
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        return new PreaggregatedMetric(0, locator, new TimeValue(1, TimeUnit.DAYS), enumRollup);
    }

    private void createEnumTestMetrics() throws Exception {
        List<IMetric> metrics = new ArrayList<IMetric>();

        Set<String> tenant1Metrics = new HashSet<String>() {{
            add(METRIC_NAME_A1);
            add(METRIC_NAME_A2);
            add(METRIC_NAME_B1);
            add(METRIC_NAME_B2);
        }};

        // add TENANT_1 metrics
        for (String metricName: tenant1Metrics) {
            metrics.add(createEnumTestMetric(TENANT_1, metricName));
        }

        Set<String> tenant2Metrics = new HashSet<String>() {{
            add(METRIC_NAME_B1);
            add(METRIC_NAME_B2);
            add(METRIC_NAME_B3);
        }};

        // add TENANT_2 metrics
        for (String metricName: tenant2Metrics) {
            metrics.add(createEnumTestMetric(TENANT_2, metricName));
        }

        createTestMetrics(TENANT_1, tenant1Metrics);
        createTestMetrics(TENANT_2, tenant2Metrics);
        createEnumData(metrics);
    }

    private void createEnumData(List<IMetric> metrics) throws IOException {
        enumElasticIO.insertDiscovery(metrics);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    private void createEnumTestMetrics(String tenantId, Map<String, List<String>> enumData) throws Exception {

        List<IMetric> enumMetrics = new ArrayList<IMetric>();
        for (Map.Entry<String, List<String>> entry : enumData.entrySet()) {
            String metricName = entry.getKey();
            List<String> enumValues = entry.getValue();

            BluefloodEnumRollup enumRollup = new BluefloodEnumRollup();
            for (String enumValue: enumValues) {
                enumRollup.withEnumValue(enumValue, 1L);
            }

            enumMetrics.add(createEnumTestMetric(tenantId, metricName, enumRollup));

        }

        createTestMetrics(tenantId, enumData.keySet());
        createEnumData(enumMetrics);
    }

    private void createSingleEnumTestMetric(String tenantId, final String enumMetricName) throws Exception {
        final List<String> newEnumValues = new ArrayList<String>() {{
            add("ev1");
            add("ev2");
        }};

        Map<String, List<String>> enumData = new HashMap<String, List<String>>() {{
            put(enumMetricName, newEnumValues);
        }};

        createEnumTestMetrics(tenantId, enumData);
    }

}
