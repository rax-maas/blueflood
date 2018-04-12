package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.joining;

/**
 * {@link TokenDiscoveryIO} implementation using elasticsearch
 */
public class ElasticTokensIO implements TokenDiscoveryIO {

    public static final String ES_DOCUMENT_TYPE = "tokens";

    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");

    protected final Timer esMetricNamesQueryTimer = Metrics.timer(getClass(), "ES Metric Names Query Duration");

    private static final Logger log = LoggerFactory.getLogger(ElasticTokensIO.class);

    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_READ);

    public ElasticsearchRestHelper elasticsearchRestHelper;

    public ElasticTokensIO() {
        this.elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
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
            int statusCode = elasticsearchRestHelper.indexTokens(tokens);
            if(statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED){
                String errorMessage =
                        String.format("Indexing tokens into elasticsearch failed with status code [%s]", statusCode);
                log.error(errorMessage);
                throw new IOException(errorMessage);
            }
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
}
