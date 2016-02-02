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
    protected final Timer getNextTokensTimer = Metrics.timer(getClass(), "getNextTokens Duration");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");
    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected Meter classCastExceptionMeter = Metrics.meter(getClass(), "Failed Cast to IMetric");
    protected Histogram queryBatchHistogram = Metrics.histogram(getClass(), "Query Batch Size");

    public static String METRICS_TOKENS_AGGREGATE = "metric_tokens";
    public static String ELASTICSEARCH_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ);

    public static String ENUMS_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_ENUMS_INDEX_NAME_WRITE);
    public static String ENUMS_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_ENUMS_INDEX_NAME_READ);

    private final int LEVEL_0 = 0;
    private final int LEVEL_1 = 1;
    private final String REGEX_TOKEN_DELIMTER = "\\.";

    public List<SearchResult> search(String tenant, String query) throws Exception {
        return search(tenant, Arrays.asList(query));
    }


    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        String[] indexes = getIndexesToSearch();

        return searchByIndexes(tenant, queries, indexes);
    }

    private List<SearchResult> searchByIndexes(String tenant, List<String> queries, String[] indexes) {
        List<SearchResult> results = new ArrayList<SearchResult>();
        Timer.Context multiSearchCtx = searchTimer.time();
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

        SearchResponse response = client.prepareSearch(indexes)
                .setRouting(tenant)
                .setSize(100000)
                .setVersion(true)
                .setQuery(bqb)
                .execute()
                .actionGet();

        multiSearchCtx.stop();

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

        int totalTokensInPrefix = 0;

        if (prefix != null && !prefix.isEmpty()) {

            //prefix is a glob and in glob's "." is not a special character.
            totalTokensInPrefix = prefix.split(REGEX_TOKEN_DELIMTER).length;
        }

        String regexforNext2Levels = regexforNextNLevels(prefix, 2);
        SearchResponse response = aggregateByMetricTokens(tenant, regexforNext2Levels);

        getPathsTimerCtx.stop();

        Terms aggregateTerms = response.getAggregations().get(METRICS_TOKENS_AGGREGATE);

        final Set<String> tokensWithNextLevel = new LinkedHashSet<String>();
        final Map<Integer, Set<String>> completeMetricNamesByLevel = new HashMap<Integer, Set<String>>();

        processMetricTokensAggregate(aggregateTerms,
                totalTokensInPrefix, tokensWithNextLevel, completeMetricNamesByLevel);

        Set<String> enumValuesAs1LevelSet = new LinkedHashSet<String>();
        Map<String, Boolean> tokensWithEnumsAs2LevelMap = new LinkedHashMap<String, Boolean>();

        for (String metricName: completeMetricNamesByLevel.get(LEVEL_1)) {
            tokensWithEnumsAs2LevelMap.put(metricName.substring(metricName.lastIndexOf(".") + 1), false);
        }

        if (completeMetricNamesByLevel.get(LEVEL_0).size() > 0 ||
                completeMetricNamesByLevel.get(LEVEL_1).size() > 0) {

            List<String> queries = new ArrayList<String>();

            //prefix is a glob. Adding '*' would get any metrics starting with the given prefix.
            queries.add(prefix + "*");

            List<SearchResult> searchResults = searchByIndexes(tenant, queries, new String[] {ENUMS_INDEX_NAME_READ});
            for (SearchResult searchResult: searchResults) {
                if (searchResult.getEnumValues() != null && !searchResult.getEnumValues().isEmpty()) {

                    String metricName = searchResult.getMetricName();
                    String[] tokens = metricName.split(REGEX_TOKEN_DELIMTER);

                    int enumMetricLevel = tokens.length - totalTokensInPrefix;

                    if (enumMetricLevel == LEVEL_0) {
                        enumValuesAs1LevelSet.addAll(searchResult.getEnumValues());
                    } else if (enumMetricLevel == LEVEL_1) {

                        String key = searchResult.getMetricName();
                        tokensWithEnumsAs2LevelMap.put(key.substring(key.lastIndexOf(".") + 1), true);
                    }
                }
            }
        }

        return prepareResults(tokensWithNextLevel,
                tokensWithEnumsAs2LevelMap, enumValuesAs1LevelSet);
    }

    /**
     *
     * @param tokensWithTwoLevels contains tokens after prefix which also have next level
     * @param tokensWithEnumsAsNextLevelMap contains tokens after prefix which have enums as next level
     * @param enumValues1LevelSet  contains enum values which the given prefix has
     * @returns a list of TokenInfo
     */
    private ArrayList<TokenInfo> prepareResults(final Set<String> tokensWithTwoLevels,
                                                final Map<String, Boolean> tokensWithEnumsAsNextLevelMap,
                                                final Set<String> enumValues1LevelSet) {

        //We add all the tokens which have the next level to resultList
        final ArrayList<TokenInfo> resultList = new ArrayList<TokenInfo>();
        for (String token : tokensWithTwoLevels) {
            resultList.add(new TokenInfo(token, true));
        }

        //indicates that there are enums for the given prefix.
        for (String enumValue: enumValues1LevelSet) {
            resultList.add(new TokenInfo(enumValue, false));
        }

        //indicates that there are enums at the second level for the given prefix.
        for (Map.Entry<String, Boolean> entry : tokensWithEnumsAsNextLevelMap.entrySet()) {
            resultList.add(new TokenInfo(entry.getKey(), entry.getValue()));
        }
        return resultList;
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
    private SearchResponse aggregateByMetricTokens(final String tenant, final String regexMetricName) {

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


    /**
     * From a given terms aggregate results, it identifies the following kind of tokens
     *
     * 1) Next level of tokens for a given prefix which also have subsequent next level (not checking for enums yet)
     * 2) For the metric names which do not fall under above category, identifies if any of them are complete metric
     *    names at a given prefix level(LEVEL_0) or at a next level(LEVEL_1).
     *
     * @param aggregateTerms
     * @param totalTokensInPrefix
     * @param tokensWithNextLevel  Next level of tokens for a given prefix which also have subsequent next level
     * @param completeMetricNamesByLevel contains complete metric names.
     */
    private void processMetricTokensAggregate(final Terms aggregateTerms,
                                              final int totalTokensInPrefix,
                                              final Set<String> tokensWithNextLevel,
                                              final Map<Integer, Set<String>> completeMetricNamesByLevel) {

        //0 indicates ground level. if prefix is foo.bar, 0 is at foo.bar level
        final Map<String, Long> partialMetrics0LevelMap = new LinkedHashMap<String, Long>();
        final Map<String, Long> partialMetrics0LevelAggMap = new LinkedHashMap<String, Long>();

        final Map<String, Long> partialMetrics1LevelMap = new LinkedHashMap<String, Long>();
        final Map<String, Long> partialMetrics1LevelAggMap = new LinkedHashMap<String, Long>();


        for (Terms.Bucket bucket: aggregateTerms.getBuckets()) {
            final String key = bucket.getKey();
            final String[] tokens = key.split(REGEX_TOKEN_DELIMTER);

            /**
             * Lets say we get these buckets with prefix of 'foo'. foo should have a doc_count that
             * matches the sum of all its next levels(only one level deep) which is 3. In the example
             * below 'foo.bar' is a bar by itself and so its count will be higher than its sublevels.
             *
             * "buckets" : [ {
             *     "key"        : "foo",
             *     "doc_count"  : 3
             * }, {
             *     "key"        : "foo.bar",
             *     "doc_count"  : 3
             * }, {
             *     "key"        : "foo.bar.baz",
             *     "doc_count"  : 1
             * }, {
             *     "key"        : "foo.bar.qux",
             *     "doc_count"  : 1
             * } ]
             *
             * For this data,
             *  partialbars0LevelMap     : {"foo" -> "3"}
             *  partialbars0LevelAggMap  : {"foo" -> "3"}
             *
             *  partialbars1LevelMap     : {"foo.bar" -> "3"}
             *  partialbars1LevelAggMap  : {"foo.bar" -> "2"}
             *
             *  tokensWithquxLevels      : {'bar'}
             */

            switch (tokens.length - totalTokensInPrefix) {
                case 2:

                    //for prefix foo, if ES query returns foo.bar.baz, we are grabbing 'bar' as next token.
                    String nextToken = tokens[totalTokensInPrefix];
                    tokensWithNextLevel.add(nextToken);

                    //calculate aggregate for foo.bar
                    aggregateCountInMap(partialMetrics1LevelAggMap,
                            bucket.getDocCount(), key.substring(0, key.lastIndexOf(".")));
                    break;

                case 1:

                    //for prefix foo, if ES query returns foo.bar, we are calculating aggregate for foo
                    partialMetrics1LevelMap.put(key, bucket.getDocCount());
                    aggregateCountInMap(partialMetrics0LevelAggMap,
                            bucket.getDocCount(), key.substring(0, key.lastIndexOf(".")));
                    break;

                case 0:

                    //for prefix foo, if ES query returns foo
                    partialMetrics0LevelMap.put(key, bucket.getDocCount());
                    break;

                default:
                    break;
            }
        }


        completeMetricNamesByLevel.put(LEVEL_0,
                findCompleteMetricNames(partialMetrics0LevelMap, partialMetrics0LevelAggMap));
        completeMetricNamesByLevel.put(LEVEL_1,
                findCompleteMetricNames(partialMetrics1LevelMap, partialMetrics1LevelAggMap));
    }

    /**
     * For the given inputMap, updates the count for the given key.
     * Adds a new entry if key is not already present.
     *
     * @param inputMap
     * @param newCount
     * @param key
     */
    private void aggregateCountInMap(final Map<String, Long> inputMap, final Long newCount, String key) {
        if (inputMap.get(key) != null) {
            inputMap.put(key, inputMap.get(key) + newCount);
        } else {
            inputMap.put(key, newCount);
        }
    }

    /**
     * Composes a list of complete metric names by comparing the doc count of the given metric name
     * to the aggregate count of its immediate next sub level.
     *
     * @param partialMetricsMap
     * @param partialMetricsAggregateMap contains the aggregate count of sublevel of metric names in partialMetricsMap
     * @return
     */
    private Set<String> findCompleteMetricNames(final Map<String, Long> partialMetricsMap,
                                                final Map<String, Long> partialMetricsAggregateMap) {

        Set<String> completeMetricNames = new HashSet<String>();
        for (Map.Entry<String, Long> entry : partialMetricsMap.entrySet()) {

            String key = entry.getKey();

            if (partialMetricsAggregateMap.get(key) == null) {

                //if the metric name has a bucket with no sub levels, its a complete metric name.
                completeMetricNames.add(key);
            } else if (partialMetricsAggregateMap.get(key) < entry.getValue()) {

                //if total doc count is greater than its sub levels, its a complete metric name
                completeMetricNames.add(key);
            }
        }
        return completeMetricNames;
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
    private String regexforNextNLevels(final String prefix, final int level) {

        //grabs chars until the next "." which is basically a token
        String regexToGrabSingleToken = "[^.]+";

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
            return regexToGrabSingleToken + REGEX_TOKEN_DELIMTER + regexToGrabSingleToken;

        } else {

            GlobPattern prefixPattern = new GlobPattern(prefix);
            String prefixRegex = prefixPattern.compiled().toString();

            if (prefix.equals("*")) {

                // get metric names which matches the given prefix and have either one or two next levels,
                // Ex: For metric foo.bar.baz.qux, if prefix=*, we should get foo.bar, foo.bar.baz. We are not
                // grabbing 0 level as it will give back bar, baz, qux because of the way data is structured.
                return prefixRegex + "(" + REGEX_TOKEN_DELIMTER + regexToGrabSingleToken + ")" + "{1," + level + "}";
            } else {

                // get metric names which matches the given prefix and have either one or two next levels,
                // Ex: For metric foo.bar.baz.qux, if prefix=foo.bar, get foo.bar, foo.bar.baz, foo.bar.baz.qux
                return prefixRegex + "(" + REGEX_TOKEN_DELIMTER + regexToGrabSingleToken + ")" + "{0," + level + "}";
            }
        }

    }

    protected abstract String[] getIndexesToSearch();

    protected abstract List<SearchResult> dedupResults(List<SearchResult> results);

    protected abstract SearchResult convertHitToMetricDiscoveryResult(SearchHit hit);

}
