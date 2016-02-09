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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

public abstract class AbstractElasticIO implements DiscoveryIO {

    protected Client client;

    // todo: these should be instances per client.
    protected final Timer searchTimer = Metrics.timer(getClass(), "Search Duration");
    protected final Timer getNextTokensTimer = Metrics.timer(getClass(), "getNextTokens ES metric index query Duration");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");
    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected Meter classCastExceptionMeter = Metrics.meter(getClass(), "Failed Cast to IMetric");
    protected Histogram queryBatchHistogram = Metrics.histogram(getClass(), "Query Batch Size");

    public static String METRICS_TOKENS_AGGREGATE = "metric_tokens";
    public static String ELASTICSEARCH_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ);

    public static String ENUMS_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_ENUMS_INDEX_NAME_WRITE);
    public static String ENUMS_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_ENUMS_INDEX_NAME_READ);

    private int MAX_RESULT_LIMIT = 100000;
    public static final String REGEX_TOKEN_DELIMTER = "\\.";

    //grabs chars until the next "." which is basically a token
    private static final String REGEX_TO_GRAB_SINGLE_TOKEN = "[^.]*";


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


        for (SearchHit hit : response.getHits().getHits()) {
            SearchResult result = convertHitToMetricDiscoveryResult(hit);
            results.add(result);
        }
        return dedupResults(results);
    }

    /**
     * If we think of metric name(Ex: foo.bar.baz) as hierarchical tokens separated by delimiter("."),
     * this API method would return the next level of tokens which follow the given prefix.
     *
     * @param tenant
     * @param prefix is glob representation
     * @return
     * @throws Exception
     */
    public List<TokenInfo> getNextTokens(final String tenant, final String prefix) throws Exception {

        Timer.Context getPathsTimerCtx = getNextTokensTimer.time();

        SearchResponse response;

        try {
            //for a given prefix, get upto next two levels of tokens
            String regexForNext2Levels = regexForNextNLevels(prefix, 2);
            response = getMetricTokensFromES(tenant, regexForNext2Levels);

        } finally {
            getPathsTimerCtx.stop();
        }

        MetricIndexData metricIndexData = buildMetricIndexData(response, prefix);


        TokenInfoListBuilder tokenInfoBuilder = new TokenInfoListBuilder();
        //tokens at next level which also have subsequent next level
        tokenInfoBuilder.addTokenWithNextLevel(metricIndexData.getTokensWithNextLevel());

        Set<String> completeMetricsAtPrefixLevel = metricIndexData.getBaseLevelCompleteMetricNames();
        Set<String> completeMetricsAtPrefixNextLevel = metricIndexData.getNextLevelCompleteMetricNames();

        //for tokens after prefix which dont have subsequent next level, initialize with next level as false.
        for (String nextLevelMetricName: completeMetricsAtPrefixNextLevel) {
            tokenInfoBuilder.addToken(nextLevelMetricName.substring(nextLevelMetricName.lastIndexOf(".") + 1), false);
        }

        //Only if there are complete metric names at either prefix level or its next level, look if they have enum values.
        if (completeMetricsAtPrefixLevel.size() > 0 || completeMetricsAtPrefixNextLevel.size() > 0) {

            searchForEnumValues(tenant, prefix, tokenInfoBuilder);
        }

        return tokenInfoBuilder.build();
    }


    /**
     * For the given prefix, if there are complete metric names at prefix level or its next level,
     * we need to determine if any of these metric names have enum values.
     *
     * If they do,for prefix level, enum values will be used as next level of tokens and for prefix
     * next level, they would be used to determine if next level tokens have subsequent next level or not.
     *
     * @param tenant
     * @param prefix
     * @param tokenInfoBuilder
     * @return
     */
    private TokenInfoListBuilder searchForEnumValues(final String tenant, String prefix,
                                                TokenInfoListBuilder tokenInfoBuilder) {

        List<String> queries = new ArrayList<String>();

        //prefix is a glob. Adding '*' would get any metrics starting with the given prefix.
        queries.add(prefix + "*");

        List<SearchResult> searchResults = searchESByIndexes(tenant, queries, new String[]{ENUMS_INDEX_NAME_READ});
        for (SearchResult searchResult: searchResults) {

            if (searchResult.getEnumValues() != null && !searchResult.getEnumValues().isEmpty()) {

                String metricName = searchResult.getMetricName();
                String[] tokens = metricName.split(REGEX_TOKEN_DELIMTER);

                int enumMetricLevel = tokens.length - getTotalTokensInPrefix(prefix);

                if (enumMetricLevel == 0) {

                    //for metrics at prefix level, enum values are grabbed as next level of tokens
                    tokenInfoBuilder.addEnumValues(searchResult.getEnumValues());
                } else if (enumMetricLevel == 1) {

                    //for metrics at prefix next level, next level tokens are marked as true
                    String key = searchResult.getMetricName();
                    tokenInfoBuilder.addToken(key.substring(key.lastIndexOf(".") + 1), true);
                }
            }
        }

        return tokenInfoBuilder;
    }

    private int getTotalTokensInPrefix(String prefix) {

        if (prefix == null || prefix.isEmpty())
            return 0;

        return prefix.split(REGEX_TOKEN_DELIMTER).length;
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
     *          "term": {
     *              "tenantId": "836986"
     *          }
     *      },
     *      "aggs": {
     *          "metric_name_tokens": {
     *              "terms": {
     *                  "field" : "metric_name",
     *                  "include": "<regex>",
     *                  "size": 0
     *              }
     *          }
     *      }
     *  }
     *
     * @param tenant
     * @param regexMetricName
     * @return
     */
    private SearchResponse getMetricTokensFromES(final String tenant, final String regexMetricName) {

        AggregationBuilder aggregationBuilder =
                AggregationBuilders.terms(METRICS_TOKENS_AGGREGATE)
                        .field(ESFieldLabel.metric_name.name())
                        .include(regexMetricName)
                        .size(0);

        return client.prepareSearch(new String[] {ELASTICSEARCH_INDEX_NAME_READ})
                .setRouting(tenant)
                .setSize(0)
                .setVersion(true)
                .setQuery(QueryBuilders.termQuery(ESFieldLabel.tenantId.toString(), tenant))
                .addAggregation(aggregationBuilder)
                .execute()
                .actionGet();
    }


    private MetricIndexData buildMetricIndexData(final SearchResponse response, final String prefix) {

        MetricIndexData metricIndexData = new MetricIndexData(getTotalTokensInPrefix(prefix));
        Terms aggregateTerms = response.getAggregations().get(METRICS_TOKENS_AGGREGATE);

        for (Terms.Bucket bucket: aggregateTerms.getBuckets()) {
            metricIndexData.add(bucket.getKey(), bucket.getDocCount());
        }

        return metricIndexData;
    }

    /**
     * Returns regex which could grab upto next n levels of metric tokens from the given
     * prefix level(including prefix level).
     *
     * (Some exceptions when prefix has only one level due to the nature of underlying data)
     *
     * @param prefix
     * @param level
     * @return
     */
    protected String regexForNextNLevels(final String prefix, final int level) {

        if (prefix == null || prefix.isEmpty()) {
            /**
             * We are trying to get first level of metric name. For a given metric foo.bar.baz, the current
             * analyzer indexes the following values.
             *
             * foo, bar, baz, foo.bar, foo.bar.baz
             *
             * To grab only the first level, we first query to get indexes which are of the form
             * <first_level_path>.<second_level_path>. We have to do this way because of 'bar' and 'baz'
             * being indexed separately.
             */

            //get metric names which have only two levels. For example: foo.bar, x.y
            return REGEX_TO_GRAB_SINGLE_TOKEN + REGEX_TOKEN_DELIMTER + REGEX_TO_GRAB_SINGLE_TOKEN;

        } else {

            GlobPattern prefixPattern = new GlobPattern(prefix);

            String prefixRegex = prefixPattern.compiled().toString().replaceAll("\\.\\*", REGEX_TO_GRAB_SINGLE_TOKEN);

            if (getTotalTokensInPrefix(prefix) == 1) {

                // get metric names which matches the given prefix and have either one or two next levels,
                // Ex: For metric foo.bar.baz.qux, if prefix=*, we should get foo.bar, foo.bar.baz. We are not
                // grabbing 0 level as it will give back bar, baz, qux because of the way data is structured.
                return prefixRegex + "(" + REGEX_TOKEN_DELIMTER + REGEX_TO_GRAB_SINGLE_TOKEN + ")" + "{1," + level + "}";
            } else {

                // get metric names which matches the given prefix and have either one or two next levels,
                // Ex: For metric foo.bar.baz.qux, if prefix=foo.bar, get foo.bar, foo.bar.baz, foo.bar.baz.qux
                return prefixRegex + "(" + REGEX_TOKEN_DELIMTER + REGEX_TO_GRAB_SINGLE_TOKEN + ")" + "{0," + level + "}";
            }
        }

    }

    protected abstract String[] getIndexesToSearch();

    protected abstract List<SearchResult> dedupResults(List<SearchResult> results);

    protected abstract SearchResult convertHitToMetricDiscoveryResult(SearchHit hit);

}

