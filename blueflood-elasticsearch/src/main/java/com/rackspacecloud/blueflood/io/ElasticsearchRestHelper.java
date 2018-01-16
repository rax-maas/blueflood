package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
        //Example URL: localhost:9200/metric_metadata/metrics/_search";
        String url = String.format("%s/%s/%s/_search?routing=%s", baseUrl,
                EventElasticSearchIO.EVENT_INDEX, EventElasticSearchIO.ES_TYPE, tenantId);

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        String queryDslString = getDslString(query);
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
            if((e instanceof HttpResponseException) && (response != null))
                logger.error("Status code: " + response.getStatusLine().getStatusCode() + " with msg: " + e.getMessage());

            throw new RuntimeException(e.getMessage(), e);
        }
        finally {
            response.close();
        }
    }

    private String getDslString(Map<String, List<String>> query) {
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
            // TODO: LOG THIS INVALID CASE
            System.out.println("This is an error case.");
            return "";
        }

        String dslString;

        if(StringUtils.isEmpty(tagsQString)) {
            dslString = "{\"query\":{\"bool\" : {\"must\": [" + rangeQueryString + "]}}}";
        }
        else{
            dslString = "{\"query\":{\"bool\" : {\"must\": [" + tagsQString + "," + rangeQueryString + "]}}}";
        }

        return dslString;
    }

    private String extractFieldFromQuery(String name, Map<String, List<String>> query) {
        String result = "";
        if (query.containsKey(name)) {
            List<String> temp = query.get(name);
            if(temp == null || temp.size() == 0) return result;
            result = temp.get(0); // TODO: why do we have list if we have to get only first item?
        }
        return result;
    }

    public String fetch(String indexName, String documentType, String tenantId, String query) throws IOException {
        //Example URL: localhost:9200/metric_metadata/metrics/_search";
        String url = String.format("%s/%s/%s/_search", baseUrl, indexName, documentType);

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(getHeaders());

        GlobPattern pattern = new GlobPattern(query);
        String queryDslString = getQueryDslString(tenantId, query, pattern.hasWildcard());

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
            if((e instanceof HttpResponseException) && (response != null))
                logger.error("Status code: " + response.getStatusLine().getStatusCode() + " with msg: " + e.getMessage());

            throw new RuntimeException(e.getMessage(), e);
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
            metricNameQ = String.format("{\"wildcard\":{\"%s\":\"%s\"}}", ESFieldLabel.metric_name.toString(), queryString);
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

        sb.append(String.format("{\"what\": \"%s\",", event.get("what")));
        sb.append(String.format("\"when\": \"%d\",", (long) event.get("when")));
        sb.append(String.format("\"tags\": \"%s\",", event.get("tags")));
        sb.append(String.format("\"data\": \"%s\"}", event.get("data")));

        return sb.toString();
    }

    private String bulkStringify(List<IMetric> metrics){
        StringBuilder sb = new StringBuilder();

        for(IMetric metric : metrics){
            Locator locator = metric.getLocator();

            sb.append(String.format(
                    "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\", \"routing\" : %s } }%n",
                    AbstractElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE, AbstractElasticIO.ELASTICSEARCH_DOCUMENT_TYPE,
                    locator.getTenantId() + ":" + locator.getMetricName(), locator.getTenantId()));

            sb.append(String.format(
                    "{ \"tenantId\" : \"%s\", \"metric_name\" : \"%s\" }%n",
                    locator.getTenantId(), locator.getMetricName()));
        }

        return sb.toString();
    }

    private void index(HttpPost httpPost, String bulkString) throws IOException {
        CloseableHttpResponse response = null;

        try{
            HttpEntity entity = new NStringEntity(bulkString, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            response = closeableHttpClient.execute(httpPost);

            logger.info("Status: " + response.getStatusLine().getStatusCode());
        }
        catch (Exception e){
            if((e instanceof HttpResponseException) && (response != null))
                logger.error("Status code: " + response.getStatusLine().getStatusCode() + " with msg: " + e.getMessage());

            throw new RuntimeException(e.getMessage(), e);
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
