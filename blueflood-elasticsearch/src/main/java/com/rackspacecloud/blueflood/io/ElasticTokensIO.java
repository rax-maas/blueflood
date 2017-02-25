package com.rackspacecloud.blueflood.io;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * {@link TokenDiscoveryIO} implementation using elastic search
 */
public class ElasticTokensIO implements TokenDiscoveryIO {

    public static final String ES_DOCUMENT_TYPE = "tokens";

    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");

    protected final Timer esMetricNamesQueryTimer = Metrics.timer(getClass(), "ES Metric Names Query Duration");

    private static final Logger log = LoggerFactory.getLogger(ElasticTokensIO.class);

    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_READ);

    private Client client;

    public ElasticTokensIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public ElasticTokensIO(Client client) {
        this.client = client;
    }

    public ElasticTokensIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    @Override
    public void insertDiscovery(Token token) throws IOException {
        List<Token> batch = new ArrayList<>();
        batch.add(token);
        insertDiscovery(batch);
    }

    @Override
    public void insertDiscovery(List<Token> tokens) throws IOException {
        batchHistogram.update(tokens.size());
        if (tokens.size() == 0) return;

        Timer.Context ctx = writeTimer.time();
        try {

            BulkRequestBuilder bulk = client.prepareBulk();

            for (Token token : tokens) {
                bulk.add(createSingleRequest(token));
            }

            bulk.execute().actionGet();
        } catch (EsRejectedExecutionException esEx) {
            log.error(("Error during bulk insert to ES with status: [" + esEx.status() + "] " +
                    "with message: [" + esEx.getDetailedMessage() + "]"));
        } finally {
            ctx.stop();
        }
    }


    IndexRequestBuilder createSingleRequest(Token token) throws IOException {

        if (StringUtils.isEmpty(token.getToken())) {
            throw new IllegalArgumentException("trying to insert token discovery without a token");
        }

        return client.prepareIndex(ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE, ES_DOCUMENT_TYPE)
                .setId(token.getId())
                .setSource(createSourceContent(token))
                .setCreate(true)
                .setRouting(token.getLocator().getTenantId());
    }

    @Override
    public List<MetricName> getMetricNames(String tenantId, String query) throws Exception {

        return searchESByIndexes(tenantId, query, getIndexesToSearch());
    }

    /**
     * This method queries elastic search for a given glob query and returns list of {@link MetricName}'s.
     *
     * @param tenantId
     * @param query is glob
     * @param indexes
     * @return
     */
    private List<MetricName> searchESByIndexes(String tenantId, String query, String[] indexes) {

        if (StringUtils.isEmpty(query)) return new ArrayList<>();

        QueryBuilder bqb = buildESQuery(tenantId, query);

        SearchResponse response;
        Timer.Context timerCtx = esMetricNamesQueryTimer.time();
        try {
            response = client.prepareSearch(indexes)
                             .setRouting(tenantId)
                             .setSize(AbstractElasticIO.MAX_RESULT_LIMIT)
                             .setVersion(true)
                             .setQuery(bqb)
                             .execute()
                             .actionGet();
        } finally {
            timerCtx.stop();
        }

        return Arrays.stream(response.getHits().getHits())
                     .map(this::convertHitToMetricNameResult)
                     .collect(toList());
    }

    /**
     * Builds ES query to grab tokens corresponding to the given query glob.
     * For a given query foo.bar.*, we would like to grab all the tokens with
     * parent as foo.bar
     *
     * Sample ES query for a query glob = foo.bar.*:
     *
     *      "query": {
     *          "bool" : {
     *              "must" : [
     *                  { "term": {  "tenantId": "<tenantId>" }},
     *                  { "term": {  "parent": "foo.bar" }}
     *              ]
     *          }
     *      }
     *
     * @param tenantId
     * @param query
     * @return
     */
    private BoolQueryBuilder buildESQuery(String tenantId, String query) {

        String[] queryTokens = query.split(Locator.metricTokenSeparatorRegex);
        String lastToken = queryTokens[queryTokens.length - 1];

        BoolQueryBuilder bqb = boolQuery();

        /**
         * Builds parent part of the query for the given input query glob tokens.
         * For a given query foo.bar.*, parent part is foo.bar
         *
         * For example:
         *
         *  For query = foo.bar.*
         *          { "term": {  "parent": "foo.bar" }}
         *
         *  For query = foo.*.*
         *          { "regexp": {  "parent": "foo.[^.]+" }}
         *
         *  For query = foo.b*.*
         *          { "regexp": {  "parent": "foo.b[^.]*" }}
         */

        QueryBuilder parentQB;
        if (queryTokens.length == 1) {
            parentQB = termQuery(ESFieldLabel.parent.name(), "");
        } else {

            String parent = Arrays.stream(queryTokens)
                                  .limit(queryTokens.length - 1)
                                  .collect(joining(Locator.metricTokenSeparator));

            GlobPattern parentGlob = new GlobPattern(parent);
            if (parentGlob.hasWildcard()) {
                parentQB = regexpQuery(ESFieldLabel.parent.name(), getRegexToHandleTokens(parentGlob));
            } else {
                parentQB = termQuery(ESFieldLabel.parent.name(), parent);
            }
        }

        bqb.must(termQuery(ESFieldLabel.tenantId.name(), tenantId))
           .must(parentQB);

        // For example: if query=foo.bar.*, we can just get every token for the parent=foo.bar
        // but if query=foo.bar.b*, we want to add the token part of the query for "b*"
        if (!lastToken.equals("*")) {

            QueryBuilder tokenQB;

            GlobPattern pattern = new GlobPattern(lastToken);
            if (pattern.hasWildcard()) {
                tokenQB = regexpQuery(ESFieldLabel.token.name(), pattern.compiled().toString());
            } else {
                tokenQB = termQuery(ESFieldLabel.token.name(), query);
            }

            bqb.must(tokenQB);
        }

        return bqb;
    }

    /**
     * For a given glob, gives regex for {@code Locator.metricTokenSeparator} separated tokens
     *
     * For example:
     *      globPattern of foo.*.* would produce a regex  foo\.[^.]+\.[^.]+
     *      globPattern of foo.b*.* would produce a regex foo\.b[^.]*\.[^.]+
     *
     * @param globPattern
     * @return
     */
    protected String getRegexToHandleTokens(GlobPattern globPattern) {
        String[] queryRegexParts = globPattern.compiled().toString().split("\\\\.");

        return Arrays.stream(queryRegexParts)
                     .map(this::convertRegexToCaptureUptoNextToken)
                     .collect(joining(Locator.metricTokenSeparatorRegex));
    }

    private String convertRegexToCaptureUptoNextToken(String queryRegex) {

        if (queryRegex.equals(".*"))
            return queryRegex.replaceAll("\\.\\*", "[^.]+");
        else
            return queryRegex.replaceAll("\\.\\*", "[^.]*");
    }

    private static XContentBuilder createSourceContent(Token token) throws IOException {
        XContentBuilder json;

        json = XContentFactory.jsonBuilder().startObject()
                              .field(ESFieldLabel.token.toString(), token.getToken())
                              .field(ESFieldLabel.parent.toString(), token.getParent())
                              .field(ESFieldLabel.isLeaf.toString(), token.isLeaf())
                              .field(ESFieldLabel.tenantId.toString(), token.getLocator().getTenantId())
                              .endObject();
        return json;
    }

    protected MetricName convertHitToMetricNameResult(SearchHit hit) {
        Map<String, Object> source = hit.getSource();

        String parent = (String)source.get(ESFieldLabel.parent.toString());
        String token = (String)source.get(ESFieldLabel.token.toString());
        boolean isCompleteName = (Boolean)source.get(ESFieldLabel.isLeaf.toString());

        StringBuilder metricName = new StringBuilder(parent);
        if (metricName.length() > 0) {
            metricName.append(Locator.metricTokenSeparator);
        }
        metricName.append(token);

        return new MetricName(metricName.toString(), isCompleteName);
    }

    protected String[] getIndexesToSearch() {
        return new String[] {ELASTICSEARCH_TOKEN_INDEX_NAME_READ};
    }
}
