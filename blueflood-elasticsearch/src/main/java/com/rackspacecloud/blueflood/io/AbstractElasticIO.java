package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.rackspacecloud.blueflood.types.Locator.METRIC_TOKEN_SEPARATOR_REGEX;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.index.query.QueryBuilders.*;

public abstract class AbstractElasticIO implements DiscoveryIO {

    protected Client client;

    // todo: these should be instances per client.
    protected final Timer searchTimer = Metrics.timer(getClass(), "Search Duration");
    protected final Timer esMetricNamesQueryTimer = Metrics.timer(getClass(), "ES Metric Names Query Duration");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");
    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected Meter classCastExceptionMeter = Metrics.meter(getClass(), "Failed Cast to IMetric");
    protected Histogram queryBatchHistogram = Metrics.histogram(getClass(), "Query Batch Size");
    private final Histogram searchResultsSizeHistogram = Metrics.histogram(getClass(), "Metrics search results size");

    public static String METRICS_TOKENS_AGGREGATE = "metric_tokens";
    public static String ELASTICSEARCH_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ);

    public static int MAX_RESULT_LIMIT = 100000;

    //grabs chars until the next "." which is basically a token
    protected static final String REGEX_TO_GRAB_SINGLE_TOKEN = "[^.]*";


    public List<SearchResult> search(String tenant, String query) throws Exception {
        return search(tenant, Arrays.asList(query));
    }


    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        String[] indexes = getIndexesToSearch();

        return searchESByIndexes(tenant, queries, indexes);
    }

    private List<SearchResult> searchESByIndexes(String tenant, List<String> queries, String[] indexes) {
        List<SearchResult> results = new ArrayList<SearchResult>();
        Timer.Context multiSearchCtx = searchTimer.time();
        SearchResponse response;
        try {
            queryBatchHistogram.update(queries.size());
            BoolQueryBuilder bqb = boolQuery();
            QueryBuilder qb;

            for (String query : queries) {
                GlobPattern pattern = new GlobPattern(query);
                if (!pattern.hasWildcard()) {
                    qb = termQuery(ESFieldLabel.metric_name.name(), query);
                } else {
                    qb = regexpQuery(ESFieldLabel.metric_name.name(), pattern.compiled().toString());
                }

                bqb.should(boolQuery()
                                .must(termQuery(ESFieldLabel.tenantId.toString(), tenant))
                                .must(qb)
                );
            }

            response = client.prepareSearch(indexes)
                    .setRouting(tenant)
                    .setSize(MAX_RESULT_LIMIT)
                    .setVersion(true)
                    .setQuery(bqb)
                    .execute()
                    .actionGet();
        } finally {
            multiSearchCtx.stop();
        }

        searchResultsSizeHistogram.update(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            SearchResult result = convertHitToMetricDiscoveryResult(hit);
            results.add(result);
        }
        return dedupResults(results);
    }

    /**
     * This method returns a list of {@link MetricName}'s matching the given glob query.
     *
     * for metrics: foo.bar.xxx,
     *              foo.bar.baz.qux,
     *
     * for query=foo.bar.*, returns the below list of metric names
     *
     * new MetricName("foo.bar.xxx", true)   <- From metric foo.bar.xxx
     * new MetricName("foo.bar.baz", false)  <- From metric foo.bar.baz.qux
     *
     * @param tenant
     * @param query is glob representation of hierarchical levels of token. Ex: foo.bar.*
     * @return
     * @throws Exception
     */
    public List<MetricName> getMetricNames(final String tenant, final String query) throws Exception {

        Timer.Context esMetricNamesQueryTimerCtx = esMetricNamesQueryTimer.time();
        SearchResponse response;

        try {
            response = getMetricNamesFromES(tenant, regexToGrabCurrentAndNextLevel(query));
        } finally {
            esMetricNamesQueryTimerCtx.stop();
        }

        // For example, if query = foo.bar.*, base level is 3 which is equal to the number of tokens in the query.
        int baseLevel = getTotalTokens(query);
        MetricIndexData metricIndexData = buildMetricIndexData(response, baseLevel);

        List<MetricName> metricNames = new ArrayList<>();

        //Metric Names matching query which have next level
        metricNames.addAll(metricIndexData.getMetricNamesWithNextLevel()
                                          .stream()
                                          .map(x -> new MetricName(x, false))
                                          .collect(toSet()));

        //complete metric names matching query
        metricNames.addAll(metricIndexData.getCompleteMetricNamesAtBaseLevel()
                                          .stream()
                                          .map(x -> new MetricName(x, true))
                                          .collect(toSet()));

        return metricNames;
    }

    private int getTotalTokens(String query) {

        if (StringUtils.isEmpty(query))
            return 0;

        return query.split(METRIC_TOKEN_SEPARATOR_REGEX).length;
    }

    /**
     * Performs terms aggregation by metric_name which returns doc_count by
     * metric_name index that matches the given regex.
     *
     *  Sample request body:
     *
     *  {
     *      "size": 0,
     *      "query": {
     *          "bool" : {
     *              "must" : [ {
     *                  "term" : {
     *                      "tenantId" : "ratanasv"
     *                  }
     *              }, {
     *                  "regexp" : {
     *                      "metric_name" : {
     *                         "value" : "<regex>"
     *                      }
     *                  }
     *              } ]
     *          }
     *      },
     *      "aggs": {
     *          "metric_name_tokens": {
     *              "terms": {
     *                  "field" : "metric_name",
     *                  "include": "<regex>",
     *                  "execution_hint": "map",
     *                  "size": 0
     *              }
     *          }
     *      }
     *  }
     *
     * The two regex expressions used in the query above would be same, one to filter
     * at query level and another to filter the aggregation buckets.
     *
     * Execution hint of "map" works by using field values directly instead of ordinals
     * in order to aggregate data per-bucket
     *
     * @param tenant
     * @param regexMetricName
     * @return
     */
    private SearchResponse getMetricNamesFromES(final String tenant, final String regexMetricName) {

        AggregationBuilder aggregationBuilder =
                AggregationBuilders.terms(METRICS_TOKENS_AGGREGATE)
                        .field(ESFieldLabel.metric_name.name())
                        .include(regexMetricName)
                        .executionHint("map")
                        .size(0);

        TermQueryBuilder tenantIdQuery = QueryBuilders.termQuery(ESFieldLabel.tenantId.toString(), tenant);
        RegexpQueryBuilder metricNameQuery = QueryBuilders.regexpQuery(ESFieldLabel.metric_name.name(), regexMetricName);

        return client.prepareSearch(new String[] {ELASTICSEARCH_INDEX_NAME_READ})
                .setRouting(tenant)
                .setSize(0)
                .setVersion(true)
                .setQuery(QueryBuilders.boolQuery().must(tenantIdQuery).must(metricNameQuery))
                .addAggregation(aggregationBuilder)
                .execute()
                .actionGet();
    }


    private MetricIndexData buildMetricIndexData(final SearchResponse response, final int baseLevel) {

        MetricIndexData metricIndexData = new MetricIndexData(baseLevel);
        Terms aggregateTerms = response.getAggregations().get(METRICS_TOKENS_AGGREGATE);

        for (Terms.Bucket bucket: aggregateTerms.getBuckets()) {
            metricIndexData.add(bucket.getKey(), bucket.getDocCount());
        }

        return metricIndexData;
    }

    /**
     * Returns regex which could grab metric names from current level to the next level
     * for a given query.
     *
     * (Some exceptions when query has only one level due to the nature of underlying data)
     *
     * for metrics : foo.bar.baz,
     *               foo.bar.baz.qux,
     *
     * for query=foo.bar.*, the regex which this method returns will capture the following metric token paths.
     *
     *  "foo.bar.baz"       <- current level
     *  "foo.bar.baz.qux"   <- next level
     *
     * @param query
     * @return
     */
    protected String regexToGrabCurrentAndNextLevel(final String query) {

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("Query(glob) string cannot be null/empty");
        }

        String queryRegex = getRegex(query);
        int totalQueryTokens = getTotalTokens(query);

        if (totalQueryTokens == 1) {

            // get metric names which matches the given query and have a next level,
            // Ex: For metric foo.bar.baz.qux, if query=*, we should get foo.bar. We are not
            // grabbing 0 level as it will give back bar, baz, qux because of the way data is structured.
            String baseRegex = convertRegexToCaptureUptoNextToken(queryRegex);
            return baseRegex + METRIC_TOKEN_SEPARATOR_REGEX + REGEX_TO_GRAB_SINGLE_TOKEN;

        } else {

            String[] queryRegexParts = queryRegex.split("\\\\.");

            String queryRegexUptoPrevLevel = StringUtils.join(queryRegexParts, METRIC_TOKEN_SEPARATOR_REGEX, 0, totalQueryTokens - 1);
            String baseRegex = convertRegexToCaptureUptoNextToken(queryRegexUptoPrevLevel);

            String queryRegexLastLevel = queryRegexParts[totalQueryTokens - 1];
            String lastTokenRegex = convertRegexToCaptureUptoNextToken(queryRegexLastLevel);

            // Ex: For metric foo.bar.baz.qux.xxx, if query=foo.bar.b*, get foo.bar.baz, foo.bar.baz.qux
            // In this case baseRegex = "foo.bar", lastTokenRegex = "b[^.]*"' and the final
            // regex is foo\.bar\.b[^.]*(\.[^.]*){0,1}
            return baseRegex +
                    METRIC_TOKEN_SEPARATOR_REGEX + lastTokenRegex +
                        "(" +
                    METRIC_TOKEN_SEPARATOR_REGEX + REGEX_TO_GRAB_SINGLE_TOKEN +
                        ")"  + "{0,1}";
        }
    }

    private String convertRegexToCaptureUptoNextToken(String queryRegex) {
        return queryRegex.replaceAll("\\.\\*", REGEX_TO_GRAB_SINGLE_TOKEN);
    }

    private String getRegex(String glob) {
        GlobPattern pattern = new GlobPattern(glob);
        return pattern.compiled().toString();
    }

    protected abstract String[] getIndexesToSearch();

    protected abstract List<SearchResult> dedupResults(List<SearchResult> results);

    protected abstract SearchResult convertHitToMetricDiscoveryResult(SearchHit hit);

}

