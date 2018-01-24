package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchRestHelper {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRestHelper.class);
    private final CloseableHttpClient closeableHttpClient;
    private final String baseUrl;
    private static int MAX_RESULT_LIMIT = 100000;

    private static final ElasticsearchRestHelper INSTANCE = new ElasticsearchRestHelper();

    public static ElasticsearchRestHelper getInstance() {
        return INSTANCE;
    }

    private ElasticsearchRestHelper(){
        logger.info("Creating a new instance of ElasticsearchRestHelper...");
        Configuration config = Configuration.getInstance();
        this.baseUrl = String.format("http://%s",
                config.getStringProperty(ElasticIOConfig.ELASTICSEARCH_HOST_FOR_REST_CLIENT));
        this.closeableHttpClient = HttpClientBuilder.create().build();
    }

    public String fetchEvents(String tenantId, Map<String, List<String>> query) throws IOException {
        String url = String.format("%s/%s/%s/_search?routing=%s&size=%d", baseUrl,
                EventElasticSearchIO.EVENT_INDEX, EventElasticSearchIO.ES_TYPE, tenantId, MAX_RESULT_LIMIT);

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        String queryDslString = getDslString(tenantId, query);
        CloseableHttpResponse response = null;

        try {
            HttpEntity httpEntity = new NStringEntity(queryDslString, ContentType.APPLICATION_JSON);
            httpPost.setEntity(httpEntity);

            response = closeableHttpClient.execute(httpPost);
            String str = EntityUtils.toString(response.getEntity());
            EntityUtils.consume(response.getEntity());
            return str;
        }
        catch(Exception e){
            if((e instanceof HttpResponseException) && (response != null)) {
                logger.error("fetchEvents failed with status code: {} and exception message: {}",
                        response.getStatusLine().getStatusCode(), e.getMessage());
            }

            throw new RuntimeException(String.format("fetchEvents failed with message: %s", e.getMessage()), e);
        }
        finally {
            response.close();
        }
    }

    private String getDslString(String tenantId, Map<String, List<String>> query) {
        String tenantIdQ = String.format("{\"term\":{\"%s\":\"%s\"}}", ESFieldLabel.tenantId.toString(), tenantId);

        if(query == null){
            return "{\"query\":{\"bool\" : {\"must\": [" + tenantIdQ + "]}}}";
        }

        String tagsValue = extractFieldFromQuery(Event.FieldLabels.tags.toString(), query);
        String untilValue = extractFieldFromQuery(Event.untilParameterName, query);
        String fromValue = extractFieldFromQuery(Event.fromParameterName, query);

        String tagsQString = "";

        if (StringUtils.isNotEmpty(tagsValue))
            tagsQString = String.format("{\"term\":{\"%s\":\"%s\"}}", Event.FieldLabels.tags.toString(), tagsValue);

        String rangeQueryString;

        if (StringUtils.isNotEmpty(untilValue) && StringUtils.isNotEmpty(fromValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"from\":%d,\"to\":%d}}}",
                    Long.parseLong(fromValue), Long.parseLong(untilValue));
        } else if (StringUtils.isNotEmpty(untilValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"to\":%d}}}", Long.parseLong(untilValue));
        } else if (StringUtils.isNotEmpty(fromValue)) {
            rangeQueryString = String.format("{\"range\":{\"when\":{\"from\":%d}}}", Long.parseLong(fromValue));
        } else {
            /*
            TODO: Find out if following statement (for logger) is true from Richard and George.
             */
            logger.error("Cannot create Query DSL. Both 'from' and 'to' parameters are empty.");
            rangeQueryString = "";
        }

        StringBuilder sb = new StringBuilder(tenantIdQ);

        if(StringUtils.isNotEmpty(tagsQString)) sb.append("," + tagsQString);
        if(StringUtils.isNotEmpty(rangeQueryString)) sb.append("," + rangeQueryString);

        String dslString = "{\"query\":{\"bool\" : {\"must\": [" + sb.toString() + "]}}}";

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

    public String fetch(String indexName, String documentType, String tenantId, String query) throws IOException {
        //Example URL: localhost:9200/metric_metadata/metrics/_search&size=50";
        String url = String.format("%s/%s/%s/_search?size=%d", baseUrl, indexName, documentType, MAX_RESULT_LIMIT);

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        GlobPattern pattern = new GlobPattern(query);
        String compiledString = pattern.compiled().toString();
        // replace one '\' char with two '\\'
        compiledString = compiledString.replaceAll("\\\\", "\\\\\\\\");
        String queryDslString = getQueryDslString(tenantId, compiledString, pattern.hasWildcard());

        CloseableHttpResponse response = null;

        try {
            HttpEntity httpEntity = new NStringEntity(queryDslString, ContentType.APPLICATION_JSON);
            httpPost.setEntity(httpEntity);

            response = closeableHttpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            String str = EntityUtils.toString(entity);
            EntityUtils.consume(entity);
            return str;
        }
        catch(Exception e){
            if((e instanceof HttpResponseException) && (response != null)) {
                logger.error("fetch failed with status code: {} and exception message: {}",
                        response.getStatusLine().getStatusCode(), e.getMessage());
            }

            throw new RuntimeException(String.format("fetch failed with message: %s", e.getMessage()), e);
        }
        finally {
            response.close();
        }
    }

    private String getQueryDslString(String tenantId, String queryString, boolean isWild){
        String dslString;

        String tenantIdQ = String.format("{\"term\":{\"%s\":\"%s\"}}", ESFieldLabel.tenantId.toString(), tenantId);
        String metricNameQ;

        if(isWild){
            metricNameQ = String.format("{\"regexp\":{\"%s\":\"%s\"}}", ESFieldLabel.metric_name.toString(), queryString);
        }
        else{
            metricNameQ = String.format("{\"term\":{\"%s\":\"%s\"}}", ESFieldLabel.metric_name.toString(), queryString);
        }

        dslString = "{\"query\":{\"bool\" : {\"must\": [" + tenantIdQ + "," + metricNameQ + "]}}}";

        return dslString;
    }

    public void indexMetrics(List<IMetric> metrics) throws IOException {
        String bulkString = bulkStringify(metrics);
        String url = String.format("%s/_bulk", baseUrl);

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        index(httpPost, bulkString);
    }

    public void indexEvent(Map<String, Object> event) throws IOException {
        String eventString = stringifyEvent(event);
        String url = String.format("%s/%s/%s?routing=%s",
                baseUrl, EventElasticSearchIO.EVENT_INDEX, EventElasticSearchIO.ES_TYPE,
                event.get(Event.FieldLabels.tenantId.toString()));

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        index(httpPost, eventString);
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

    private String bulkStringify(List<IMetric> metrics){
        StringBuilder sb = new StringBuilder();

        for(IMetric metric : metrics){
            Locator locator = metric.getLocator();

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

    private void index(HttpPost httpPost, String bulkString) throws IOException {
        CloseableHttpResponse response = null;

        try{
            HttpEntity entity = new NStringEntity(bulkString, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            response = closeableHttpClient.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();

            if(statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED){
                logger.error("index method failed with status code: {} and error: {}",
                        response.getStatusLine().getStatusCode(),
                        EntityUtils.toString(response.getEntity()));
                System.out.println(EntityUtils.toString(response.getEntity()));
            }
        }
        catch (Exception e){
            if((e instanceof HttpResponseException) && (response != null)) {
                logger.error("index method failed with status code: {} and exception message: {}",
                        response.getStatusLine().getStatusCode(), e.getMessage());
            }

            throw new RuntimeException(String.format("index method failed with message: %s", e.getMessage()), e);
        }
        finally {
            response.close();
        }
    }

    private Header[] getHeaders(){
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put("Accept", "application/json");
        headersMap.put("Content-Type", "application/json");

        Header[] headers = new Header[headersMap.size()];
        int i = 0;
        for(String key : headersMap.keySet()){
            headers[i++] = new BasicHeader(key, headersMap.get(key));
        }
        return headers;
    }
}
