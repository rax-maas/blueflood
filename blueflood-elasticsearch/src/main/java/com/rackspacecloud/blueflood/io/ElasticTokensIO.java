package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.cache.TokenCache;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * {@link DiscoveryIO} implementation that indexes tokens to Elasticsearch.
 */
public class ElasticTokensIO implements DiscoveryIO {

    public static final String ES_DOCUMENT_TYPE = "tokens";

    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected final Meter tokenCount = Metrics.meter(getClass(), "Tokens Written");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");

    protected final Timer esMetricNamesQueryTimer = Metrics.timer(getClass(), "ES Metric Names Query Duration");

    private static final Logger log = LoggerFactory.getLogger(ElasticTokensIO.class);

    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_READ);

    public ElasticsearchRestHelper elasticsearchRestHelper;

    public ElasticTokensIO() {
        this.elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
    }

    public void setElasticsearchRestHelper(ElasticsearchRestHelper elasticsearchRestHelper) {
        this.elasticsearchRestHelper = elasticsearchRestHelper;
    }

    @VisibleForTesting
    public void insertDiscovery(Token token) throws IOException {
        List<Token> batch = new ArrayList<>();
        batch.add(token);
        insertDiscovery(batch);
    }

    /**
     * A hacky de-generified implementation of two semi-compatible methods from the interfaces implemented here. One
     * interface expects a list of metrics and the other a list of tokens. This is from an unfortunate copy/paste
     * incident in the dark past. The {@link com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter} calls this
     * to create discovery data for metrics as they're ingested. Originally, it was a bad copy of that class that called
     * this and passed a list of tokens. Now this lives behind DiscoveryWriter as a discovery module, as appropriate,
     * and it receives a list of metrics, as it should. Tests still exist that send it a list of tokens. Once those
     * tests are fixed, this method can change to its correct signature of accepting a list of metrics.
     */
    public void insertDiscovery(List things) throws IOException {
        if (things.isEmpty()) {
            return;
        }
        if (things.get(0) instanceof Token) {
            insertTokens(things);
        } else if (things.get(0) instanceof IMetric) {
            insertMetrics(things);
        } else {
            throw new IllegalArgumentException("Unknown type of thing to insert: " + things.get(0).getClass().getName());
        }
    }

    public void insertMetrics(List<IMetric> metrics) throws IOException {
        insertTokens(Token.getUniqueTokens(metrics.stream().map(IMetric::getLocator))
                .filter(token -> !TokenCache.getInstance().isTokenCurrent(token))
                .collect(Collectors.toList()));
    }

    public void insertTokens(List<Token> tokens) throws IOException {
        batchHistogram.update(tokens.size());
        tokenCount.mark(tokens.size());
        if (tokens.size() == 0) return;

        Timer.Context ctx = writeTimer.time();
        try {
            elasticsearchRestHelper.indexTokens(tokens);
            // Update token cache to reduce future writes
            tokens.stream()
                    // Don't cache leaf nodes. Those are the complete locators which are already covered by the
                    // LocatorCache in DiscoveryWriter. Caching them again here will just use up extra memory.
                    .filter(token -> !token.isLeaf())
                    .forEach(token -> TokenCache.getInstance().setTokenCurrent(token));
        } finally {
            ctx.stop();
        }
    }

    @Override
    public List<MetricName> getMetricNames(String tenantId, String query) throws Exception {
        return searchESByIndexes(tenantId, query, getIndexesToSearch());
    }

    /**
     * This method queries elasticsearch for a given glob query and returns list of {@link MetricName}'s.
     *
     * @param tenantId
     * @param query is glob
     * @param indexes
     * @return
     */
    private List<MetricName> searchESByIndexes(String tenantId, String query, String[] indexes) {
        if (StringUtils.isEmpty(query)) return new ArrayList<>();

        Timer.Context timerCtx = esMetricNamesQueryTimer.time();
        try {
            String response = elasticsearchRestHelper.fetchTokenDocuments(indexes, tenantId, query);

            List<MetricName> metricNames = getMetricNames(response);
            Set<MetricName> uniqueMetricNames = new HashSet<>(metricNames);
            return new ArrayList<>(uniqueMetricNames);
        } catch (IOException e) {
            log.error("IOException: Elasticsearch token query failed for tenantId {}. {}", tenantId, e.getMessage());
            throw new RuntimeException(String.format("searchESByIndexes failed with message: %s", e.getMessage()), e);
        } catch (Exception e) {
            log.error("Exception: Elasticsearch token query failed for tenantId {}. {}", tenantId, e.getMessage());
            throw new RuntimeException(String.format("searchESByIndexes failed with message: %s", e.getMessage()), e);
        } finally {
            timerCtx.stop();
        }
    }

    private List<MetricName> getMetricNames(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response);

        Iterator<JsonNode> iter = root.get("hits").get("hits").elements();

        List<MetricName> metricNames = new ArrayList<>();
        while(iter.hasNext()){
            JsonNode source = iter.next().get("_source");

            String parent = source.get(ESFieldLabel.parent.toString()).asText();
            String token = source.get(ESFieldLabel.token.toString()).asText();
            boolean isCompleteName = source.get(ESFieldLabel.isLeaf.toString()).asBoolean();

            StringBuilder metricName = new StringBuilder(parent);
            if (metricName.length() > 0) {
                metricName.append(Locator.METRIC_TOKEN_SEPARATOR);
            }
            metricName.append(token);

            metricNames.add(new MetricName(metricName.toString(), isCompleteName));
        }
        return metricNames;
    }

    /**
     * For a given glob, gives regex for {@code Locator.METRIC_TOKEN_SEPARATOR} separated tokens
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
                     .collect(joining(Locator.METRIC_TOKEN_SEPARATOR_REGEX));
    }

    private String convertRegexToCaptureUptoNextToken(String queryRegex) {

        if (queryRegex.equals(".*"))
            return queryRegex.replaceAll("\\.\\*", "[^.]+");
        else
            return queryRegex.replaceAll("\\.\\*", "[^.]*");
    }

    protected String[] getIndexesToSearch() {
        return new String[] {ELASTICSEARCH_TOKEN_INDEX_NAME_READ};
    }

    @Override
    public void insertDiscovery(IMetric metric) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<SearchResult> search(String tenant, String query) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        throw new UnsupportedOperationException("Not implemented");
    }
}
