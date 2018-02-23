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
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.rackspacecloud.blueflood.io.AbstractElasticIO.ELASTICSEARCH_INDEX_NAME_READ;
import static com.rackspacecloud.blueflood.io.ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_READ;
import static org.junit.Assert.assertEquals;
import static java.util.stream.Collectors.toList;

public class ElasticIOIntegrationTest extends BaseElasticTest {

    protected static ElasticIO elasticIO;
    protected static ElasticTokensIO elasticTokensIO;

    @BeforeClass
    public static void setup() throws Exception {
        esSetup = new EsSetup();

        elasticIO = new ElasticIO();
        elasticIO.insertDiscovery(createTestMetrics(TENANT_A));
        elasticIO.insertDiscovery(createTestMetrics(TENANT_B));
        elasticIO.insertDiscovery(createTestMetricsFromInterface(TENANT_C));

        //inserting to metric_tokens
        elasticTokensIO = new ElasticTokensIO();
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_A));
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_B));
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_C));

        addTestMetrics(TENANT_A, new HashSet<String>() {{
            add("foo.bar.baz");
        }});

        addTestMetrics(TENANT_A, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});

        int statusCodeMetrics = elasticIO.elasticsearchRestHelper.refreshIndex(ELASTICSEARCH_INDEX_NAME_READ);
        if(statusCodeMetrics != 200) {
            System.out.println(String.format("Refresh for %s failed with status code: %d",
                    ELASTICSEARCH_INDEX_NAME_READ, statusCodeMetrics));
        }

        refreshTokensIndex();
    }

    private static void addTestMetrics(String tenantId, Set<String> fullyQualifiedMetricNames) throws Exception {

        List<IMetric> metrics = new ArrayList<>();
        for (String metricName: fullyQualifiedMetricNames) {
            metrics.add(new Metric(Locator.createLocatorFromPathComponents(tenantId, metricName),
                    5647382910L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT));
        }

        insertDiscovery(metrics);
    }

    private static void refreshTokensIndex() throws IOException {
        int statusCodeTokens = elasticIO.elasticsearchRestHelper.refreshIndex(ELASTICSEARCH_TOKEN_INDEX_NAME_READ);
        if(statusCodeTokens != 200) {
            System.out.println(String.format("Refresh for %s failed with status code: %d",
                    ELASTICSEARCH_TOKEN_INDEX_NAME_READ, statusCodeTokens));
        }
    }

    private static List<Token> createTestTokens(String tenantId) {
        return Token.getUniqueTokens(createComplexTestLocators(tenantId).stream())
                .collect(toList());
    }

    protected void createTestMetrics(String tenantId, Set<String> fullyQualifiedMetricNames) throws Exception {

        List<IMetric> metrics = new ArrayList<>();
        for (String metricName: fullyQualifiedMetricNames) {
            metrics.add(new Metric(Locator.createLocatorFromPathComponents(tenantId, metricName),
                    5647382910L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT));
        }

        insertDiscovery(metrics);
    }

    /*
    Once done testing, delete all of the records of the given type and index.
    NOTE: Don't delete the index or the type, because that messes up the ES settings.
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
        List<String> typesToEmpty = new ArrayList<>();
        typesToEmpty.add("/metric_metadata/metrics/_query");
        typesToEmpty.add("/metric_tokens/tokens/_query");

        for (String typeToEmpty : typesToEmpty)
            deleteAllDocuments(typeToEmpty);
    }

    private static void deleteAllDocuments(String typeToEmpty) throws URISyntaxException, IOException {
        URIBuilder builder = new URIBuilder().setScheme("http")
                .setHost("127.0.0.1").setPort(9200)
                .setPath(typeToEmpty);

        HttpEntityEnclosingRequestBase delete = new HttpEntityEnclosingRequestBase() {
            @Override
            public String getMethod() {
                return "DELETE";
            }
        };
        delete.setURI(builder.build());

        String deletePayload = "{\"query\":{\"match_all\":{}}}";
        HttpEntity entity = new NStringEntity(deletePayload, ContentType.APPLICATION_JSON);
        delete.setEntity(entity);

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = client.execute(delete);
        if(response.getStatusLine().getStatusCode() != 200)
        {
            System.out.println("Couldn't delete index after running tests.");
        }
        else {
            System.out.println("Successfully deleted index after running tests.");
        }
    }

    //@Override
    protected static void insertDiscovery(List<IMetric> metrics) throws IOException {
        elasticIO.insertDiscovery(metrics);

        Stream<Locator> locators = metrics.stream().map(IMetric::getLocator);
        elasticTokensIO.insertDiscovery(Token.getUniqueTokens(locators).collect(toList()));
    }

    @Test(expected=NullPointerException.class)
    public void testElasticsearchRestHelperIndex() throws IOException {
        ElasticsearchRestHelper elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
        elasticsearchRestHelper.index(null, null);
    }

    @Test
    public void testElasticsearchRestHelperInvalidUrlReturns404() throws IOException {

        String tenantId = "test1";
        String metricName = "one.two.three.four.five";
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        locatorMap.put(tenantId, new ArrayList<>());
        locatorMap.get(tenantId).add(locator);

        Metric metric =
                new Metric(locator, 123456789L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT);

        ElasticsearchRestHelper elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
        elasticsearchRestHelper.setBaseUrlForTestOnly("www.google.com");
        List<IMetric> metrics = new ArrayList<>();
        metrics.add(metric);

        int statusCode = elasticsearchRestHelper.indexMetrics(metrics);
        elasticsearchRestHelper.setBaseUrlForTestOnly("127.0.0.1:9200");

        Assert.assertEquals(404, statusCode);
    }

    @Test
    public void testCreateSingleRequest_WithNotNullMetricName() throws Exception {
        String tenantId = "test1";
        String metricName = "one.two.three.four.five";
        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        locatorMap.put(tenantId, new ArrayList<>());
        locatorMap.get(tenantId).add(locator);

        Metric metric =
                new Metric(locator, 123456789L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT);
        elasticIO.insertDiscovery(metric);

        int statusCode = elasticIO.elasticsearchRestHelper.refreshIndex(ELASTICSEARCH_INDEX_NAME_READ);

        if(statusCode != 200) {
            System.out.println(String.format("Refresh for %s failed with status code: %d",
                    ELASTICSEARCH_INDEX_NAME_READ, statusCode));
        }

        List<SearchResult> results;
        results = elasticIO.search(tenantId, metricName);
        List<Locator> locators = locatorMap.get(tenantId);
        assertEquals(locators.size(), results.size());
        for (Locator l : locators) {
            SearchResult entry =  new SearchResult(tenantId, l.getMetricName(), "");

            boolean isFound = false;
            for(SearchResult item : results){
                if((StringUtils.isNotEmpty(item.getMetricName()) && item.getMetricName().equalsIgnoreCase(entry.getMetricName())) &&
                        (StringUtils.isNotEmpty(item.getTenantId()) && item.getTenantId().equalsIgnoreCase(entry.getTenantId()))){
                    isFound = true;
                    break;
                }
            }
            Assert.assertTrue(isFound);
        }
    }

    @Test
    public void testNoCrossTenantResults() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "*");
        assertEquals(NUM_DOCS + 2, results.size());
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
        ArrayList<String> queries = new ArrayList<>();
        queries.add(query1);
        queries.add(query2);
        results = elasticIO.search(tenantId, queries);
        assertEquals(results.size(), 2); //we searched for 2 unique metrics
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
        assertEquals(results.size(), 32);
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
        assertEquals(results.size(), 2);
    }

    public void testWildcard(String tenantId, String unit) throws Exception {
        SearchResult entry;
        List<SearchResult> results;
        results = elasticIO.search(tenantId, "one.two.*");
        List<Locator> locators = locatorMap.get(tenantId);
        assertEquals(locators.size(), results.size());
        for (Locator locator : locators) {
            entry =  new SearchResult(tenantId, locator.getMetricName(), unit);

            boolean isFound = false;
            for(SearchResult item : results){
                if((StringUtils.isNotEmpty(item.getMetricName()) && item.getMetricName().equalsIgnoreCase(entry.getMetricName())) &&
                        (StringUtils.isNotEmpty(item.getTenantId()) && item.getTenantId().equalsIgnoreCase(entry.getTenantId()))){
                    isFound = true;
                    break;
                }
            }
            Assert.assertTrue(isFound);
        }

        results = elasticIO.search(tenantId, "*.fourA.*");
        assertEquals(NUM_PARENT_ELEMENTS * NUM_GRANDCHILD_ELEMENTS, results.size());
        for (int x = 0; x < NUM_PARENT_ELEMENTS; x++) {
            for (int z = 0; z < NUM_GRANDCHILD_ELEMENTS; z++) {
                entry = createExpectedResult(tenantId, x, "A", z, unit);
                Assert.assertTrue(results.contains(entry));
            }
        }

        results = elasticIO.search(tenantId, "*.three1*.four*.five2");
        assertEquals(10 * CHILD_ELEMENTS.size(), results.size());
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
        assertEquals(results.size(), 2);
        results.contains(new SearchResult(TENANT_A, "one.two.three00.fourA.five0", UNIT));
        results.contains(new SearchResult(TENANT_A, "one.two.three01.fourA.five0", UNIT));
    }

    @Test
    public void testGlobMatching2() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "one.two.three0?.fourA.five0");
        List<SearchResult> results2 = elasticIO.search(TENANT_A, "one.two.three0[0-9].fourA.five0");
        assertEquals(10, results.size());
        for (SearchResult result : results) {
            Assert.assertTrue(result.getMetricName().startsWith("one.two.three"));
            assertEquals(result.getTenantId(), TENANT_A);
            results2.contains(result);
        }
    }

    @Test
    public void testGlobMatching3() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "one.two.three0[01].fourA.five0");
        assertEquals(2, results.size());
        for (SearchResult result : results) {
            Assert.assertTrue(result.getMetricName().equals("one.two.three00.fourA.five0") || result.getMetricName().equals("one.two.three01.fourA.five0"));
        }
    }

    @Test
    public void testDeDupMetrics() throws Exception {
        // New index name and the locator to be written to it
        String ES_DUP = ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE + "_2";
        Locator testLocator = createTestLocator(TENANT_A, 0, "A", 0);
        // Metric is already there in old
        List<SearchResult> results = elasticIO.search(TENANT_A, testLocator.getMetricName());
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getMetricName(), testLocator.getMetricName());

        // Actually create the new index
        // Insert metric into the new index
        elasticIO.setINDEX_NAME_WRITE(ES_DUP);
        ArrayList metricList = new ArrayList();
        metricList.add(new Metric(createTestLocator(TENANT_A, 0, "A", 0), 987654321L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT));
        elasticIO.insertDiscovery(metricList);

        elasticIO.setINDEX_NAME_READ("metric_metadata_read");
        results = elasticIO.search(TENANT_A, testLocator.getMetricName());
        // Should just be one result
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getMetricName(), testLocator.getMetricName());
        elasticIO.setINDEX_NAME_READ(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ.getDefaultValue());
        elasticIO.setINDEX_NAME_WRITE(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE.getDefaultValue());
    }

    @Test
    public void testRegexLevel0() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticIO.regexToGrabCurrentAndNextLevel("*"));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(1, matchingTerms.size());
        assertEquals("foo.bar", matchingTerms.get(0));
    }

    @Test
    public void testRegexLevel1() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<String>();
        Pattern patternToGet2Levels = Pattern.compile(elasticIO.regexToGrabCurrentAndNextLevel("foo.*"));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(2, matchingTerms.size());
        assertEquals("foo.bar", matchingTerms.get(0));
        assertEquals("foo.bar.baz", matchingTerms.get(1));
    }

    @Test
    public void testRegexLevel2() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.qux", "foo.bar.baz.qux.quux");

        List<String> matchingTerms = new ArrayList<String>();
        Pattern patternToGet2Levels = Pattern.compile(elasticIO.regexToGrabCurrentAndNextLevel("foo.bar.*"));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(2, matchingTerms.size());
        assertEquals("foo.bar.baz", matchingTerms.get(0));
        assertEquals("foo.bar.baz.qux", matchingTerms.get(1));
    }

    @Test
    public void testRegexLevel3() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.qux", "foo.bar.baz.qux.quux");

        List<String> matchingTerms = new ArrayList<String>();
        Pattern patternToGet2Levels = Pattern.compile(elasticIO.regexToGrabCurrentAndNextLevel("foo.bar.baz.*"));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(2, matchingTerms.size());
        assertEquals("foo.bar.baz.qux", matchingTerms.get(0));
        assertEquals("foo.bar.baz.qux.quux", matchingTerms.get(1));
    }

    @Test
    public void testGetMetricNames() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";


        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", 2, results.size());

        for(MetricName metricName : results) {
            if(metricName.getName().equals("one") || metricName.getName().equals("foo")) {
                assertEquals("isCompleteName for token", false, metricName.isCompleteName());
            }
            else{
                Assert.fail(String.format("Unexpected metricName [%s]", metricName.getName()));
            }
        }
    }

    @Test
    public void testGetMetricNamesMultipleMetrics() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one|false");
            add("foo|false");
        }};

        assertEquals("Invalid total number of results", 2, results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    public void testGetMetricNamesSingleLevelPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", 2, results.size());

        for(MetricName metricName : results) {
            if(metricName.getName().equals("one.two") || metricName.getName().equals("one.foo")) {
                assertEquals("Next level indicator wrong for token", false, metricName.isCompleteName());
            }
            else {
                Assert.fail(String.format("Unexpected metricName [%s]", metricName.getName()));
            }
        }
    }

    @Test
    public void testGetMetricNamesWithWildCardPrefixMultipleLevels() throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two|false");
            add("one.foo|false");
            add("foo.bar|false");
        }};

        assertEquals("Invalid total number of results", 3, results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    public void testGetMetricNamesWithMultiLevelPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());
        }
    }

    @Test
    public void testGetMetricNamesWithWildCardPrefixAtTheEnd() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.three*.*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS * CHILD_ELEMENTS.size(), results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());;
        }
    }

    @Test
    public void testGetMetricNamesWithWildCardAndBracketsPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.{two,foo}.[ta]hree00.*";


        createTestMetrics(tenantId, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});

        refreshTokensIndex();
        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two.three00.fourA|false");
            add("one.two.three00.fourB|false");
            add("one.two.three00.fourC|false");
            add("one.foo.three00.bar|false");
        }};

        assertEquals("Invalid total number of results", CHILD_ELEMENTS.size() + 1, results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    public void testGetMetricNamesWithMultiWildCardPrefix() throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*.*";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS + 2, results.size());
        for (MetricName metricName : results) {
            if(metricName.getName().equalsIgnoreCase("foo.bar.baz")){
                Assert.assertTrue("isCompleteName value", metricName.isCompleteName());
            }
            else {
                Assert.assertFalse("isCompleteName value", metricName.isCompleteName());
            }
        }
    }

    @Test
    public void testGetMetricNamesWithoutWildCard() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.foo.three00.bar.baz";

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.foo.three00.bar.baz|true");
        }};

        assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyResults(results, expectedResults);
    }
}
