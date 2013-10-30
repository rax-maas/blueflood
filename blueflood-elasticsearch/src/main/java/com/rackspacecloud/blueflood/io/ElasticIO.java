/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import static com.rackspacecloud.blueflood.io.ElasticIO.ESFieldLabel.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;

public class ElasticIO implements DiscoveryIO {
    static enum ESFieldLabel {
        METRIC_NAME,
        TENANT_ID,
        TYPE,
        UNIT;
    }

    private final Client client;
    private static final String ES_TYPE = "metrics";
    private static final Logger log = LoggerFactory.getLogger(DiscoveryIO.class);

    private final int NUM_INDICES = Configuration.getIntegerProperty("ELASTICSEARCH_NUM_INDICES");
    public static final String INDEX_PREFIX = "test-index-";

    public ElasticIO(ElasticClientManager manager) {
        this.client = manager.getClient();
    }

    private static String createQueryString(String tenantId, Discovery md) {
        StringBuilder builder = new StringBuilder();
        builder.append(TENANT_ID.toString() + ":" + tenantId);

        if (md.getMetricName() != null) {
            builder.append(" && ");
            builder.append(METRIC_NAME.toString() + ":" + md.getMetricName());
        }

        for (Map.Entry<String, Object> entry : md.getAnnotation().entrySet()) {
            builder.append(" && ");
            builder.append(entry.getKey() + ":" + entry.getValue());
        }
        return builder.toString();
    }

    private static Map<String, Object> extractUsefulInformation(Metric metric) {
        Map<String, Object> info = new HashMap<String, Object>();
        if (metric.getUnit() != null) { // metric units may be null
            info.put(UNIT.toString(), metric.getUnit());
        }
        return info;
    }

    private static Result convertHitToMetricDiscoveryResult(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        String metricName = (String)source.get(METRIC_NAME.toString());

        String unit = (String)source.get(UNIT.toString());
        Result result = new Result(metricName, unit);

        return result;
    }

    public void insertDiscovery(List<Metric> batch) throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        for (Metric metric : batch) {
            Locator locator = metric.getLocator();
            Discovery md = new Discovery()
                .withMetricName(locator.getMetricName())
                .withAnnotation(extractUsefulInformation(metric));
            bulk.add(createSingleRequest(locator.getTenantId(), md));
        }
        bulk.execute();
    }

    public void insertDiscovery(String tenantId, Discovery discovery) throws IOException {
        createSingleRequest(tenantId, discovery).execute();
    }


    private IndexRequestBuilder createSingleRequest(String tenantId, Discovery md) throws IOException {
        if (md.getMetricName() == null) {
            throw new IllegalArgumentException("trying to insert metric discovery without a metricName");
        }
        XContentBuilder content;
        content = createSourceContent(tenantId, md);
        return client.prepareIndex(getIndex(tenantId), ES_TYPE)
                .setId(getId(tenantId, md))
                .setRouting(getRouting(tenantId))
                .setSource(content);
    }

    private XContentBuilder createSourceContent(String tenantId, Discovery md) throws IOException {
        XContentBuilder json;

        json = XContentFactory.jsonBuilder().startObject()
                    .field(TENANT_ID.toString(), tenantId)
                    .field(METRIC_NAME.toString(), md.getMetricName());


        for (Map.Entry<String, Object> entry : md.getAnnotation().entrySet()) {
            json = json.field(entry.getKey(), entry.getValue());
        }
        json = json.endObject();
        return json;
    }

    private String getId(String tenantId, Discovery md) {
        return tenantId + md.toString();
    }

    private String getIndex(String tenantId) {
        return INDEX_PREFIX + String.valueOf(Math.abs(tenantId.hashCode() % NUM_INDICES));
    }

    /** All requests from the same tenant should go to the same shard.
     * @param tenantId
     * @return
     */
    private String getRouting(String tenantId) {
        return tenantId;
    }

    public List<Result> search(String tenantId, Discovery md) {
        List<Result> result = new ArrayList<Result>();
        String queryString = createQueryString(tenantId, md);
        SearchResponse searchRes = client.prepareSearch(getIndex(tenantId))
            .setSize(500)
            .setRouting(getRouting(tenantId))
            .setVersion(true)
            .setQuery(QueryBuilders.queryString(queryString).analyzeWildcard(true))
            .execute()
            .actionGet();
        for (SearchHit hit : searchRes.getHits().getHits()) {
            Result entry = convertHitToMetricDiscoveryResult(hit);
            result.add(entry);
        }
        return result;
    }

    public static class Discovery {
        private Map<String, Object> annotation = new HashMap<String, Object>();
        private String metricName;

        public Map<String, Object> getAnnotation() {
            return annotation;
        }

        public String getMetricName() {
            return metricName;
        }

        @Override
        public String toString() {
            return "ElasticMetricDiscovery [metricName=" + metricName + ", annotation="
                    + annotation.toString() + "]";
        }

        public Discovery withAnnotation(Map<String, Object> annotation) {
            this.annotation = annotation;
            return this;
        }

        public Discovery withMetricName(String name){
            this.metricName = name;
            return this;
        }
    }

    public static class Result {
        private final String metricName;
        private final String unit;

        public Result(String name, String unit) {
            this.metricName = name;
            this.unit = unit;
        }

        public String getMetricName() {
            return metricName;
        }
        public String getUnit() {
            return unit;
        }
        @Override
        public String toString() {
            return "Result [metricName=" + metricName + ", unit=" + unit + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((metricName == null) ? 0 : metricName.hashCode());
            result = prime * result + ((unit == null) ? 0 : unit.hashCode());
            return result;
        }

        public boolean equals(Result other) {
            if (!metricName.equals(other.metricName)) {
                return false;
            } else if (!unit.equals(other.unit)) {
                return false;
            }
            return true;
        }
    }
}
