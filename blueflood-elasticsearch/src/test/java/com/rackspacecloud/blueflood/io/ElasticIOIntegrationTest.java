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

import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith( JUnitParamsRunner.class )
public class ElasticIOIntegrationTest extends BaseElasticTest {

    protected ElasticIO elasticIO;
    protected ElasticTokensIO elasticTokensIO;

    @Before
    public void setup() throws Exception {
        helper = ElasticsearchRestHelper.getInstance();
        tearDown();

        elasticIO = new ElasticIO();

        elasticIO.insertDiscovery(createTestMetrics(TENANT_A));
        elasticIO.insertDiscovery(createTestMetrics(TENANT_B));
        elasticIO.insertDiscovery(createTestMetricsFromInterface(TENANT_C));


        String TOKEN_INDEX_NAME_OLD = ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE + "_v1";

        elasticTokensIO = new ElasticTokensIO() {
            @Override
            protected String[] getIndexesToSearch() {
                return new String[] {ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_READ,
                        TOKEN_INDEX_NAME_OLD};
            }
        };

        //inserting to metric_tokens
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_A));
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_B));

        // Following code is deviation from using batch insertion just to improve the code coverage#
        // At this time, it's not so smart, as this is getting called for every test method.
        // I had to pick between two - minimum code change with code coverage improvement VS efficient change
        // with code coverage improvement. Given this check-in contains huge load of changes, I pick minimize code
        // change in test code at the cost of efficiency.
        List<Token> tokens  = createTestTokens(TENANT_C);
        for(Token token : tokens){
            elasticTokensIO.insertDiscovery(token);
        }

        //inserting same tokens to old version of metric_tokens
        this.insertTokenDiscovery(createTestTokens(TENANT_A), TOKEN_INDEX_NAME_OLD, elasticTokensIO.elasticsearchRestHelper);
        this.insertTokenDiscovery(createTestTokens(TENANT_B), TOKEN_INDEX_NAME_OLD, elasticTokensIO.elasticsearchRestHelper);
        this.insertTokenDiscovery(createTestTokens(TENANT_C), TOKEN_INDEX_NAME_OLD, elasticTokensIO.elasticsearchRestHelper);

        Thread.sleep(5 * 1000);

        helper.refreshIndex("metric_metadata");
        helper.refreshIndex("metric_tokens");
    }

    @After
    public void tearDown() throws Exception {
        List<String> typesToEmpty = new ArrayList<>();
        typesToEmpty.add("/metric_metadata/metrics/_query");
        typesToEmpty.add("/metric_tokens/tokens/_query");
        typesToEmpty.add("/metric_tokens_v1/tokens/_query");

        for (String typeToEmpty : typesToEmpty)
            deleteAllDocuments(typeToEmpty);
    }

    private void deleteAllDocuments(String typeToEmpty) throws URISyntaxException, IOException {
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
            System.out.println(String.format("Couldn't delete index [%s] after running tests.", typeToEmpty));
        }
        else {
            System.out.println(String.format("Successfully deleted [%s] index after running tests.", typeToEmpty));
        }
    }

    private List<Token> createTestTokens(String tenantId) {
        return Token.getUniqueTokens(createComplexTestLocators(tenantId).stream())
                .collect(toList());
    }

    public void indexTokens(List<Token> tokens, String indexName,
                           ElasticsearchRestHelper elasticsearchRestHelper) throws IOException {
        String bulkString = bulkStringifyTokens(tokens, indexName);
        String urlFormat = "%s/_bulk";
        elasticsearchRestHelper.index(urlFormat, bulkString);
    }

    private String bulkStringifyTokens(List<Token> tokens, String indexName){
        StringBuilder sb = new StringBuilder();

        for(Token token : tokens){
            sb.append(String.format(
                    "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\", \"routing\" : \"%s\" } }%n",
                    indexName, ElasticTokensIO.ES_DOCUMENT_TYPE,
                    token.getId(), token.getLocator().getTenantId()));

            sb.append(String.format(
                    "{ \"%s\" : \"%s\", \"%s\" : \"%s\", \"%s\" : \"%s\", \"%s\" : \"%s\" }%n",
                    ESFieldLabel.token.toString(), token.getToken(),
                    ESFieldLabel.parent.toString(), token.getParent(),
                    ESFieldLabel.isLeaf.toString(), token.isLeaf(),
                    ESFieldLabel.tenantId.toString(), token.getLocator().getTenantId()));
        }

        return sb.toString();
    }

    public void insertTokenDiscovery(List<Token> tokens, String indexName,
                                     ElasticsearchRestHelper elasticsearchRestHelper) throws IOException {
        if (tokens.size() == 0) return;
        this.indexTokens(tokens, indexName, elasticsearchRestHelper);
    }

    @Override
    protected void insertDiscovery(List<IMetric> metrics) throws IOException {
        elasticIO.insertDiscovery(metrics);

        Stream<Locator> locators = metrics.stream().map(IMetric::getLocator);
        elasticTokensIO.insertDiscovery(Token.getUniqueTokens(locators)
                .collect(toList()));
    }

    @Test
    public void testNoCrossTenantResults() throws Exception {
        List<SearchResult> results = elasticIO.search(TENANT_A, "*");
        assertEquals(NUM_DOCS, results.size());
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
            Assert.assertTrue((results.contains(entry)));
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

        // Insert metric into the new index
        elasticIO.setINDEX_NAME_WRITE(ES_DUP);
        List<IMetric> metricList = new ArrayList();
        metricList.add(new Metric(createTestLocator(TENANT_A, 0, "A", 0), 987654321L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT));

        // Calling insertDiscovery with single metric in loop rather than metrics collection to improve on code coverage.
        for(IMetric metric : metricList){
            elasticIO.insertDiscovery(metric);
        }

        helper.refreshIndex(ES_DUP);
        elasticIO.setINDEX_NAME_READ("metric_metadata_read");
        results = elasticIO.search(TENANT_A, testLocator.getMetricName());
        // Should just be one result
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getMetricName(), testLocator.getMetricName());
        elasticIO.setINDEX_NAME_READ(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ.getDefaultValue());
        elasticIO.setINDEX_NAME_WRITE(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE.getDefaultValue());
    }

    private MetricNameSearchIO getDiscoveryIO(String type) {
        if (type.equalsIgnoreCase("elasticTokensIO")) {
            return elasticTokensIO;
        } else {
            return elasticIO;
        }
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNames(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "*";


        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", 1, results.size());
        assertEquals("Next token mismatch", "one", results.get(0).getName());
        assertEquals("isCompleteName for token", false, results.get(0).isCompleteName());
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesMultipleMetrics(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});
        Thread.sleep(5 * 1000);

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one|false");
            add("foo|false");
        }};

        assertEquals("Invalid total number of results", 2, results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesSingleLevelPrefix(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "one.*";

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", 1, results.size());
        assertEquals("Next token mismatch", "one.two", results.get(0).getName());
        assertEquals("Next level indicator wrong for token", false, results.get(0).isCompleteName());
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithWildCardPrefixMultipleLevels(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});
        Thread.sleep(5 * 1000);

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two|false");
            add("foo.bar|false");
        }};

        assertEquals("Invalid total number of results", 2, results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithMultiLevelPrefix(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.*";

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());
        }
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithWildCardPrefixAtTheEnd(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "one.two.three*.*";

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS * CHILD_ELEMENTS.size(), results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());;
        }
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithWildCardAndBracketsPrefix(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "one.{two,foo}.[ta]hree00.*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});
        Thread.sleep(5 * 1000);

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

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
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithMultiWildCardPrefix(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*.*";

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());;
        }
    }

    @Test
    @Parameters({"elasticIO", "elasticTokensIO"})
    public void testGetMetricNamesWithoutWildCard(String type) throws Exception {
        String tenantId = TENANT_A;
        String query = "one.foo.three00.bar.baz";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});
        Thread.sleep(5 * 1000);

        List<MetricName> results = getDiscoveryIO(type).getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.foo.three00.bar.baz|true");
        }};

        assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyResults(results, expectedResults);
    }

    @Test
    public void testRegexLevel0() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<String>();
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
}