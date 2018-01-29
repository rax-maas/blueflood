package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.rackspacecloud.blueflood.types.Locator.METRIC_TOKEN_SEPARATOR_REGEX;

public abstract class AbstractElasticIO implements DiscoveryIO {
    private static final Logger log = LoggerFactory.getLogger(AbstractElasticIO.class);

    protected ElasticsearchRestHelper elasticsearchRestHelper;
    protected static final String ELASTICSEARCH_DOCUMENT_TYPE = "metrics";

    protected final Timer searchTimer = Metrics.timer(getClass(), "Search Duration");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");
    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected Meter classCastExceptionMeter = Metrics.meter(getClass(), "Failed Cast to IMetric");
    protected Histogram queryBatchHistogram = Metrics.histogram(getClass(), "Query Batch Size");
    private final Histogram searchResultsSizeHistogram = Metrics.histogram(getClass(), "Metrics search results size");

    public static String ELASTICSEARCH_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ);

    //grabs chars until the next "." which is basically a token
    protected static final String REGEX_TO_GRAB_SINGLE_TOKEN = "[^.]*";


    public List<SearchResult> search(String tenant, String query) throws Exception {
        return search(tenant, Arrays.asList(query));
    }

    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        return searchUsingRestApi(tenant, queries);
    }

    private List<SearchResult> searchUsingRestApi(String tenant, List<String> queries) {
        List<SearchResult> results = new ArrayList<>();
        Timer.Context multiSearchCtx = searchTimer.time();
        int hitsCount;

        try {
            queryBatchHistogram.update(queries.size());
            String response = elasticsearchRestHelper.fetch(
                    ELASTICSEARCH_INDEX_NAME_READ, ELASTICSEARCH_DOCUMENT_TYPE, tenant, queries);

            List<SearchResult> searchResults = getSearchResults(response);
            hitsCount = searchResults.size();
            results.addAll(searchResults);
        } catch (JsonProcessingException e) {
            log.error("Elasticsearch metrics query failed for tenantId {}. {}", tenant, e.getMessage());
            throw new RuntimeException(String.format("searchUsingRestApi failed with message: %s", e.getMessage()), e);
        } catch (IOException e) {
            log.error("Elasticsearch metrics query failed for tenantId {}. {}", tenant, e.getMessage());
            throw new RuntimeException(String.format("searchUsingRestApi failed with message: %s", e.getMessage()), e);
        } finally {
            multiSearchCtx.stop();
        }

        searchResultsSizeHistogram.update(hitsCount);

        return dedupResults(results);
    }

    private List<SearchResult> getSearchResults(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response);

        Iterator<JsonNode> iter = root.get("hits").get("hits").elements();

        List<SearchResult> searchResults = new ArrayList<>();
        while(iter.hasNext()){
            JsonNode source = iter.next().get("_source");
            String metricName = source.get(ESFieldLabel.metric_name.toString()).asText();
            String tenantId = source.get(ESFieldLabel.tenantId.toString()).asText();
            String unit = null;
            if(source.has(ESFieldLabel.unit.toString()))
                unit = source.get(ESFieldLabel.unit.toString()).asText();

            SearchResult result = new SearchResult(tenantId, metricName, unit);
            searchResults.add(result);
        }
        return searchResults;
    }

    private int getTotalTokens(String query) {

        if (StringUtils.isEmpty(query))
            return 0;

        return query.split(METRIC_TOKEN_SEPARATOR_REGEX).length;
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

    protected abstract List<SearchResult> dedupResults(List<SearchResult> results);
}

