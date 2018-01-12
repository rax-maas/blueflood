package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;


/**
 * The current scope gives us one cluster for all test methods in the test.
 * All indices and templates are deleted between each test.
 *
 * The following flags have to be set while running this test
 * -Dtests.jarhell.check=false (to handle some bug in intellij https://github.com/elastic/elasticsearch/issues/14348)
 * -Dtests.security.manager=false (https://github.com/elastic/elasticsearch/issues/16459)
 *
 */
public class ElasticTokensIOIntegrationTest extends BaseElasticTest {

    protected ElasticTokensIO elasticTokensIO;


    @Before
    public void setup() throws IOException, ExecutionException, InterruptedException {

        createIndexAndMapping(ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE,
                              ElasticTokensIO.ES_DOCUMENT_TYPE,
                              getTokensMapping());

        String TOKEN_INDEX_NAME_OLD = ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE + "_v1";

        createIndexAndMapping(TOKEN_INDEX_NAME_OLD,
                              ElasticTokensIO.ES_DOCUMENT_TYPE,
                              getTokensMapping());

        elasticTokensIO = new ElasticTokensIO(getClient()) {
            @Override
            protected String[] getIndexesToSearch() {
                return new String[] {ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_READ,
                        TOKEN_INDEX_NAME_OLD};
            }
        };

        //inserting to metric_tokens
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_A));
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_B));
        elasticTokensIO.insertDiscovery(createTestTokens(TENANT_C));

        //inserting same tokens to old version of metric_tokens
        this.insertTokenDiscovery(createTestTokens(TENANT_A), TOKEN_INDEX_NAME_OLD, getClient());
        this.insertTokenDiscovery(createTestTokens(TENANT_B), TOKEN_INDEX_NAME_OLD, getClient());
        this.insertTokenDiscovery(createTestTokens(TENANT_C), TOKEN_INDEX_NAME_OLD, getClient());

        refreshChanges();
    }

    private List<Token> createTestTokens(String tenantId) {
        return Token.getUniqueTokens(createComplexTestLocators(tenantId).stream())
                    .collect(toList());
    }

    public void insertTokenDiscovery(List<Token> tokens, String indexName, Client esClient) throws IOException {
        if (tokens.size() == 0) return;

        BulkRequestBuilder bulk = esClient.prepareBulk();

        for (Token token : tokens) {
            bulk.add(createSingleRequest(token, indexName, esClient));
        }

        bulk.execute().actionGet();
    }


    IndexRequestBuilder createSingleRequest(Token token, String indexName, Client esClient) throws IOException {

        return esClient.prepareIndex(indexName, ElasticTokensIO.ES_DOCUMENT_TYPE)
                       .setId(token.getId())
                       .setSource(ElasticTokensIO.createSourceContent(token))
                       .setCreate(true)
                       .setRouting(token.getLocator().getTenantId());
    }

    @Override
    protected void insertDiscovery(List<IMetric> metrics) throws IOException {

        Stream<Locator> locators = metrics.stream().map(IMetric::getLocator);
        elasticTokensIO.insertDiscovery(Token.getUniqueTokens(locators)
                                             .collect(toList()));
    }

    @Test
    public void testGetMetricNames() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";


        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        assertEquals("Invalid total number of results", 1, results.size());
        assertEquals("Next token mismatch", "one", results.get(0).getName());
        assertEquals("isCompleteName for token", false, results.get(0).isCompleteName());
    }

    @Test
    public void testGetMetricNamesMultipleMetrics() throws Exception {
        String tenantId = TENANT_A;
        String query = "*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});

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

        assertEquals("Invalid total number of results", 1, results.size());
        assertEquals("Next token mismatch", "one.two", results.get(0).getName());
        assertEquals("Next level indicator wrong for token", false, results.get(0).isCompleteName());
    }

    @Test
    public void testGetMetricNamesWithWildCardPrefixMultipleLevels() throws Exception {
        String tenantId = TENANT_A;
        String query = "*.*";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("foo.bar.baz");
        }});

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.two|false");
            add("foo.bar|false");
        }};

        assertEquals("Invalid total number of results", 2, results.size());
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

        assertEquals("Invalid total number of results", NUM_PARENT_ELEMENTS, results.size());
        for (MetricName metricName : results) {
            Assert.assertFalse("isCompleteName value", metricName.isCompleteName());;
        }
    }

    @Test
    public void testGetMetricNamesWithoutWildCard() throws Exception {
        String tenantId = TENANT_A;
        String query = "one.foo.three00.bar.baz";

        createTestMetrics(tenantId, new HashSet<String>() {{
            add("one.foo.three00.bar.baz");
        }});

        List<MetricName> results = elasticTokensIO.getMetricNames(tenantId, query);

        Set<String> expectedResults = new HashSet<String>() {{
            add("one.foo.three00.bar.baz|true");
        }};

        assertEquals("Invalid total number of results", expectedResults.size(), results.size());
        verifyResults(results, expectedResults);
    }


}
