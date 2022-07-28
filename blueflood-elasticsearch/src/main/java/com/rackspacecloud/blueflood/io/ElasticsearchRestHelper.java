package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.rackspacecloud.blueflood.utils.Metrics.counter;
import static com.rackspacecloud.blueflood.utils.Metrics.registerGauge;
import static java.util.stream.Collectors.joining;

public class ElasticsearchRestHelper {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRestHelper.class);
    private final CloseableHttpClient closeableHttpClient;
    private PoolingHttpClientConnectionManager pool;
    private String[] baseUrlArray;
    private int baseUrlIndex;
    private int numberOfElasticsearchEndpoints;
    private int MAX_CALL_COUNT = 10;
    private int MAX_RESULT_LIMIT = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_DISCOVERY_RESULT_SIZE);
    private static final Header defaultHttpHeaders[] = {
            new BasicHeader("Accept", "application/json"),
            new BasicHeader("Content-Type", "application/json")
    };
    private Map<String,Counter> errorCounters = new HashMap<String, Counter>() {{
       put("refreshIndex", counter(getClass(), "refreshIndex Error"));
       put("fetchEvents", counter(getClass(), "fetchEvents Error"));
       put("fetchTokenDocuments", counter(getClass(), "fetchTokenDocuments Error"));
       put("fetchDocuments", counter(getClass(), "fetchDocuments Error"));
       put("index", counter(getClass(), "index Error"));
    }};

    private static final ElasticsearchRestHelper INSTANCE = new ElasticsearchRestHelper();

    /**
     * Gets an instance of the ES rest helper for use in main source.
     * @return
     */
    public static ElasticsearchRestHelper getInstance() {
        return INSTANCE;
    }

    private ElasticsearchRestHelper(){
        logger.info("Creating a new instance of ElasticsearchRestHelper...");
        Configuration config = Configuration.getInstance();
        String[] endpoints = config.getStringProperty(ElasticIOConfig.ELASTICSEARCH_HOST_FOR_REST_CLIENT).split(",");

        initializeBaseUrlCollection(endpoints);

        this.pool = new PoolingHttpClientConnectionManager();
        int maxThreadsPerRoute = config.getIntegerProperty(ElasticIOConfig.ELASTICSEARCH_HTTP_CLIENT_THREADS_PER_ROUTE);
        pool.setDefaultMaxPerRoute(maxThreadsPerRoute);
        pool.setMaxTotal(endpoints.length * maxThreadsPerRoute);
        this.closeableHttpClient = HttpClients.custom().setConnectionManager(pool).build();
        this.initPoolSizeMetrics();
    }

    private void initPoolSizeMetrics() {
        try {
            registerGauge(ElasticsearchRestHelper.class, () -> pool.getTotalStats().getAvailable(), "Available Connections");
            registerGauge(ElasticsearchRestHelper.class, () -> pool.getTotalStats().getPending(), "Pending Connections");
            registerGauge(ElasticsearchRestHelper.class, () -> pool.getTotalStats().getLeased(), "Leased Connections");
        } catch (Exception ex) {
            logger.error("Error in registering HTTP Pool connection metrics", ex);
        }
    }

    private String getNextBaseUrl(){
        return String.format("http://%s", baseUrlArray[getNextIndexSync()]);
    }

    private synchronized int getNextIndexSync(){
        if(baseUrlIndex >= numberOfElasticsearchEndpoints) baseUrlIndex = 0;
        return baseUrlIndex++;
    }

    private void initializeBaseUrlCollection(String[] endpoints) {
        baseUrlArray = new String[endpoints.length];
        numberOfElasticsearchEndpoints = endpoints.length;

        for(int i = 0; i < numberOfElasticsearchEndpoints; i++){
            baseUrlArray[i] = endpoints[i].trim();
        }

        baseUrlIndex = 0;
    }

    public String fetchEvents(String tenantId, Map<String, List<String>> query) throws IOException {
        String url = String.format("%s/%s/_search?routing=%s&size=%d", EventElasticSearchIO.EVENT_INDEX,
                EventElasticSearchIO.ES_TYPE, tenantId, MAX_RESULT_LIMIT);

        String queryDslString = getDslString(tenantId, query);
        return executePost("%s/"+url, queryDslString, "fetchEvents").response;
    }

    private String getDslString(String tenantId, Map<String, List<String>> query) {
        String tenantIdQ = getTermQueryString(ESFieldLabel.tenantId.toString(), tenantId);

        if(query == null){
            return "{\"query\":{\"bool\" : {\"must\": [" + tenantIdQ + "]}}}";
        }

        String tagsValue = extractFieldFromQuery(Event.FieldLabels.tags.toString(), query);
        String untilValue = extractFieldFromQuery(Event.untilParameterName, query);
        String fromValue = extractFieldFromQuery(Event.fromParameterName, query);

        String tagsQString = "";

        if (StringUtils.isNotEmpty(tagsValue))
            tagsQString = getTermQueryString(Event.FieldLabels.tags.toString(), tagsValue);

        String rangeQueryString;

        if (StringUtils.isNotEmpty(untilValue) && StringUtils.isNotEmpty(fromValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"from\":%d,\"to\":%d}}}",
                    Long.parseLong(fromValue), Long.parseLong(untilValue));
        } else if (StringUtils.isNotEmpty(untilValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"to\":%d}}}", Long.parseLong(untilValue));
        } else if (StringUtils.isNotEmpty(fromValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"from\":%d}}}", Long.parseLong(fromValue));
        } else {
            logger.info("In query DSL, both 'from' and 'to' parameters are empty.");
            rangeQueryString = "";
        }

        StringBuilder sb = new StringBuilder(tenantIdQ);

        if(StringUtils.isNotEmpty(tagsQString)) sb.append("," + tagsQString);
        if(StringUtils.isNotEmpty(rangeQueryString)) sb.append("," + rangeQueryString);

        List<String> strings = new ArrayList<>();
        strings.add(sb.toString());

        String dslString = getBoolQueryString(getMustQueryString(strings));

        return dslString;
    }

    private String extractFieldFromQuery(String name, Map<String, List<String>> query) {
        String result = "";
        if (query.containsKey(name)) {
            List<String> temp = query.get(name);
            if(temp == null || temp.size() == 0) return result;
            result = temp.get(0);
        }
        return result;
    }

    public String fetch(String indexName, String documentType, String tenantId, List<String> queries) throws IOException {
        String queryDslString = getQueryDslString(tenantId, queries);
        return fetchDocuments(indexName, documentType, tenantId, queryDslString);
    }

    public String fetchTokenDocuments(String[] indices, String tenantId, String query) throws IOException {
        String queryDslString = getTokenQueryDslString(tenantId, query);
        String multiIndexString = String.join(",", indices);

        String temp = String.format("%s/_search?routing=%s&size=%d", multiIndexString, tenantId, MAX_RESULT_LIMIT);
        String urlFormat = "%s/" + temp;

        return executePost(urlFormat, queryDslString, "fetchTokenDocuments").response;
    }

    public String fetchDocuments(String indexName, String documentType, String tenantId, String queryDslString) throws IOException {
        //Example URL: localhost:9200/metric_metadata/metrics/_search?routing=123456&size=100
        String temp = String.format("%s/%s/_search?routing=%s&size=%d",
                indexName, documentType, tenantId, MAX_RESULT_LIMIT);
        String urlFormat = "%s/" + temp;

        return executePost(urlFormat, queryDslString, "fetchDocuments").response;
    }

    private String getQueryDslString(String tenantId, List<String> queries){
        List<String> mustStrings = new ArrayList<>();
        String tenantIdQString = getTermQueryString(ESFieldLabel.tenantId.toString(), tenantId);
        mustStrings.add(tenantIdQString);
        String mustValueString = getMustValueString(mustStrings);

        List<String> shouldStrings = new ArrayList<>();
        for(String query : queries) {
            String metricNameQString;
            GlobPattern pattern = new GlobPattern(query);

            if (pattern.hasWildcard()) {
                String compiledString = pattern.compiled().toString();
                // replace one '\' char with two '\\'
                compiledString = compiledString.replaceAll("\\\\", "\\\\\\\\");
                metricNameQString = getRegexpQueryString(ESFieldLabel.metric_name.toString(), compiledString);
            }
            else {
                metricNameQString = getTermQueryString(ESFieldLabel.metric_name.toString(), query);
            }
            shouldStrings.add(metricNameQString);
        }

        String shouldValueString = getShouldValueString(shouldStrings);

        String dslString = getBoolQueryString(mustValueString, shouldValueString);
        return dslString;
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
    private String getTokenQueryDslString(String tenantId, String query) {
        String[] queryTokens = query.split(Locator.METRIC_TOKEN_SEPARATOR_REGEX);
        String lastToken = queryTokens[queryTokens.length - 1];

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

        String parentString = getParentString(queryTokens);
        String tenantIdTermQueryString = getTermQueryString(ESFieldLabel.tenantId.name(), tenantId);
        List<String> mustNodes = new ArrayList<>();
        mustNodes.add(tenantIdTermQueryString);
        mustNodes.add(parentString);

        // For example: if query=foo.bar.*, we can just get every token for the parent=foo.bar
        // but if query=foo.bar.b*, we want to add the token part of the query for "b*"
        if (!lastToken.equals("*")) {

            String tokenQString;

            GlobPattern pattern = new GlobPattern(lastToken);

            if (pattern.hasWildcard()) {
                String compiledString = pattern.compiled().toString();
                tokenQString = getRegexpQueryString(ESFieldLabel.token.name(), compiledString);
            } else {
                tokenQString = getTermQueryString(ESFieldLabel.token.name(), lastToken);
            }

            mustNodes.add(tokenQString);
        }

        String mustQueryString = getMustQueryString(mustNodes);
        String boolQueryString = getBoolQueryString(mustQueryString);
        // replace one '\' char with two '\\'
        boolQueryString = boolQueryString.replaceAll("\\\\", "\\\\\\\\");

        return boolQueryString;
    }

    private String getParentString(String[] queryTokens) {
        if (queryTokens.length == 1) return getTermQueryString(ESFieldLabel.parent.name(), "");

        String parent = Arrays.stream(queryTokens)
                .limit(queryTokens.length - 1)
                .collect(joining(Locator.METRIC_TOKEN_SEPARATOR));

        GlobPattern parentGlob = new GlobPattern(parent);

        String parentQueryString;
        if (parentGlob.hasWildcard()) {
            parentQueryString = getRegexpQueryString(ESFieldLabel.parent.name(), getRegexToHandleTokens(parentGlob));
        } else {
            parentQueryString = getTermQueryString(ESFieldLabel.parent.name(), parent);
        }

        return parentQueryString;
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

    public void indexMetrics(List<IMetric> metrics) throws IOException {
        String bulkString = bulkStringify(metrics);
        String urlFormat = "%s/_bulk";
        index(urlFormat, bulkString);
    }

    public void indexTokens(List<Token> tokens) throws IOException {
        String bulkString = bulkStringifyTokens(tokens);
        String urlFormat = "%s/_bulk";
        index(urlFormat, bulkString);
    }

    public void indexEvent(Map<String, Object> event) throws IOException {
        String eventString = stringifyEvent(event);
        String temp = String.format("%s/%s?routing=%s",
                EventElasticSearchIO.EVENT_INDEX, EventElasticSearchIO.ES_TYPE,
                event.get(Event.FieldLabels.tenantId.toString()));

        String urlFormat = "%s/" + temp;

        index(urlFormat, eventString);
    }

    private String stringifyEvent(Map<String, Object> event){
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("{\"what\": \"%s\",", event.get(Event.FieldLabels.what.toString())));
        sb.append(String.format("\"when\": \"%d\",", (long) event.get(Event.FieldLabels.when.toString())));
        sb.append(String.format("\"tags\": \"%s\",", event.get(Event.FieldLabels.tags.toString())));
        sb.append(String.format("\"tenantId\": \"%s\",", event.get(Event.FieldLabels.tenantId.toString())));
        sb.append(String.format("\"data\": \"%s\"}", event.get(Event.FieldLabels.data.toString())));

        return sb.toString();
    }

    private String bulkStringifyTokens(List<Token> tokens){
        StringBuilder sb = new StringBuilder();

        for(Token token : tokens){
            sb.append(String.format(
                    "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\", \"routing\" : \"%s\" } }%n",
                    ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE, ElasticTokensIO.ES_DOCUMENT_TYPE,
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

    private String bulkStringify(List<IMetric> metrics){
        StringBuilder sb = new StringBuilder();

        for(IMetric metric : metrics){
            Locator locator = metric.getLocator();

            if(locator.getMetricName() == null)
                throw new IllegalArgumentException("trying to insert metric discovery without a metricName");

            sb.append(String.format(
                    "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\", \"routing\" : \"%s\" } }%n",
                    AbstractElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE, AbstractElasticIO.ELASTICSEARCH_DOCUMENT_TYPE,
                    locator.getTenantId() + ":" + locator.getMetricName(), locator.getTenantId()));

            if(metric instanceof Metric && getUnit((Metric) metric) != null){
                sb.append(String.format(
                        "{ \"%s\" : \"%s\", \"%s\" : \"%s\", \"%s\" : \"%s\" }%n",
                        ESFieldLabel.tenantId.toString(), locator.getTenantId(),
                        ESFieldLabel.metric_name.toString(), locator.getMetricName(),
                        ESFieldLabel.unit.toString(), getUnit((Metric) metric)));
            }
            else {
                sb.append(String.format(
                        "{ \"%s\" : \"%s\", \"%s\" : \"%s\" }%n",
                        ESFieldLabel.tenantId.toString(), locator.getTenantId(),
                        ESFieldLabel.metric_name.toString(), locator.getMetricName()));
            }
        }

        return sb.toString();
    }

    private String getUnit(Metric metric) {
        return metric.getUnit();
    }

    //If index() fails for whatever reason, it always throws IOException because indexing failed for Elasticsearch.
    public void index(final String urlFormat, final String bulkString) throws IOException {
        ExecuteResponse response = executePost(urlFormat, bulkString, "index");
        if(response.statusCode != HttpStatus.SC_OK && response.statusCode != HttpStatus.SC_CREATED) {
            throw new IOException("Elasticsearch indexing failed with status code: [" + response.statusCode + "]");
        }
    }

    private String getTermQueryString(String key, String value){
        return String.format("{\"term\":{\"%s\":\"%s\"}}", key, value);
    }

    private String getRegexpQueryString(String key, String value){
        return String.format("{\"regexp\":{\"%s\":\"%s\"}}", key, value);
    }

    private String getMustQueryString(List<String> termStrings){
        return String.format("{\"must\":%s}", getMustValueString(termStrings));
    }

    private String getMustValueString(List<String> termStrings){
        String termsString = String.join(",", termStrings);
        return String.format("[%s]", termsString);
    }

    private String getShouldValueString(List<String> termStrings){
        String termsString = String.join(",", termStrings);
        return String.format("[%s]", termsString);
    }

    private String getBoolQueryString(String mustString){
        return String.format("{\"query\":{\"bool\":%s}}", mustString);
    }

    private String getBoolQueryString(String mustValueString, String shouldValueString){
        return String.format("{\"query\":{\"bool\":{\"must\":%s,\"should\":%s,\"minimum_should_match\": 1}}}",
                mustValueString, shouldValueString);
    }

    @VisibleForTesting
    public int refreshIndex(String indexName) throws IOException {
        String url = String.format("http://%s/%s/_refresh", baseUrlArray[0], indexName);
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            response = closeableHttpClient.execute(httpGet);
        } catch (IOException e) {
            incrementErrorCounter("refreshIndex");
            logger.error("Refresh for index {} failed with status code: {} and exception message: {}",
                    indexName, response.getStatusLine().getStatusCode(), e.getMessage());
        }
        finally {
            response.close();
        }

        return response.getStatusLine().getStatusCode();
    }

    /*
        Here I am using Queue to keep a round-robin selection of next base url. If current base URL fails for
        whatever reason (with response == null), then in catch block, I am enqueueing the next base URL so that
        in next iteration call picks up the new URL. If URL works, queue will be empty and loop will break out.
    */
    private ExecuteResponse executePost(String urlFormat, String queryDslString, String methodName) throws IOException {
        String tempUrl = String.format(urlFormat, getNextBaseUrl());
        Queue<String> callQ = new LinkedList<>();
        callQ.add(tempUrl);
        int callCount = 0;
        int responseCode = 0;
        while (!callQ.isEmpty() && callCount < MAX_CALL_COUNT) {
            callCount++;
            String url = callQ.remove();

            logger.debug("Using url [{}]", url);
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeaders(defaultHttpHeaders);
            HttpEntity httpEntity = new NStringEntity(queryDslString, ContentType.APPLICATION_JSON);
            httpPost.setEntity(httpEntity);
            CloseableHttpResponse response = null;

            try {
                response = closeableHttpClient.execute(httpPost);
                responseCode = response.getStatusLine().getStatusCode();
                HttpEntity entity = response.getEntity();
                String str = EntityUtils.toString(entity);
                EntityUtils.consume(entity);
                return new ExecuteResponse(str, responseCode);
            } catch (Exception e) {
                incrementErrorCounter(methodName);
                if (response == null) {
                    logger.error("{} failed with message: {}", methodName, e.getMessage());
                    url = String.format(urlFormat, getNextBaseUrl());
                    callQ.add(url);
                } else {
                    logger.error("{} failed with status code: {} and exception message: {}", methodName,
                            response.getStatusLine().getStatusCode(), e.getMessage());
                }
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }

        return new ExecuteResponse("", responseCode);
    }

    private void incrementErrorCounter(String methodName) {
        Counter counter = errorCounters.get(methodName);
        if(counter != null) {
            counter.inc();
        } else {
            // Forgot to declare the metric? Register on demand
            counter(getClass(), String.format("%s Error", methodName)).inc();
        }
    }


    private static class ExecuteResponse {
        private String response;
        private int statusCode;

        public ExecuteResponse(String response, int statusCode) {
            this.response = response;
            this.statusCode = statusCode;
        }
    }

    @VisibleForTesting
    public Map<String,Counter> getErrorCounters() {
        return errorCounters;
    }
}
