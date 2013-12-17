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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Metrics;

import com.codahale.metrics.Timer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.rackspacecloud.blueflood.io.ElasticIO.ESFieldLabel.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ElasticIO implements DiscoveryIO {
    static enum ESFieldLabel {
        METRIC_NAME,
        TENANT_ID,
        TYPE,
        UNIT
    }

    private static final Logger log = LoggerFactory.getLogger(DiscoveryIO.class);
    private final Client client;
    private static final String ES_TYPE = "metrics";
    private static final String INDEX_PREFIX = "blueflood-";
    private final int NUM_INDICES = Configuration.getInstance().getIntegerProperty(ElasticIOConfig.ELASTICSEARCH_NUM_INDICES);
    private final Timer searchTimer = Metrics.timer(ElasticIO.class, "Search Duration");

    public static String getIndexPrefix() {
        return INDEX_PREFIX;
    }

    public ElasticIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public ElasticIO(Client client) {
        this.client = client;
    }

    public ElasticIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    private static Result convertHitToMetricDiscoveryResult(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        String metricName = (String)source.get(METRIC_NAME.toString());
        String tenantId = (String)source.get(TENANT_ID.toString());
        String unit = (String)source.get(UNIT.toString());
        Result result = new Result(tenantId, metricName, unit);

        return result;
    }

    public void insertDiscovery(List<Metric> batch) throws IOException {
        // TODO: check bulk insert result and retry
        BulkRequestBuilder bulk = client.prepareBulk();
        for (Metric metric : batch) {
            Locator locator = metric.getLocator();
            Discovery md = new Discovery(locator.getTenantId(), locator.getMetricName());
            Map<String, Object> info = new HashMap<String, Object>();
            if (metric.getUnit() != null) { // metric units may be null
                info.put(UNIT.toString(), metric.getUnit());
            }
            info.put(TYPE.toString(), metric.getType());
            md.withAnnotation(info);
            bulk.add(createSingleRequest(md));
        }
        bulk.execute().actionGet();
    }

    private IndexRequestBuilder createSingleRequest(Discovery md) throws IOException {
        if (md.getMetricName() == null) {
            throw new IllegalArgumentException("trying to insert metric discovery without a metricName");
        }
        return client.prepareIndex(getIndex(md.getTenantId()), ES_TYPE)
                .setId(md.getDocumentId())
                .setRouting(md.getRouting())
                .setSource(md.createSourceContent());
    }

    private String getIndex(String tenantId) {
        return INDEX_PREFIX + String.valueOf(Math.abs(tenantId.hashCode() % NUM_INDICES));
    }

    private static QueryBuilder createQuery(Discovery md) {
        BoolQueryBuilder qb = boolQuery()
                .must(termQuery(TENANT_ID.toString(), md.getTenantId()));
        String metricName = md.getMetricName();
        if (metricName.contains("*")) {
            qb.must(wildcardQuery("RAW_" + METRIC_NAME.toString(), metricName));
        } else {
            qb.must(termQuery("RAW_" + METRIC_NAME.toString(), metricName));
        }
        for (Map.Entry<String, Object> entry : md.getAnnotation().entrySet()) {
            qb.should(termQuery(entry.getKey(), entry.getValue()));
        }
        return qb;
    }

    public List<Result> search(Discovery md) {
        List<Result> result = new ArrayList<Result>();
        QueryBuilder query = createQuery(md);
        Timer.Context searchTimerCtx = searchTimer.time();
        SearchResponse searchRes = client.prepareSearch(getIndex(md.getTenantId()))
                .setSize(500)
                .setRouting(md.getRouting())
                .setVersion(true)
                .setQuery(query)
                .execute()
                .actionGet();
        searchTimerCtx.stop();
        for (SearchHit hit : searchRes.getHits().getHits()) {
            Result entry = convertHitToMetricDiscoveryResult(hit);
            result.add(entry);
        }
        return result;
    }

    public static class Discovery {
        private Map<String, Object> annotation = new HashMap<String, Object>();
        private final String metricName;
        private final String tenantId;

        public Discovery(String tenantId, String metricName) {
            this.tenantId = tenantId;
            this.metricName = metricName;
        }
        public Map<String, Object> getAnnotation() {
            return annotation;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getMetricName() {
            return metricName;
        }

        public String getRouting() {
            return tenantId;
        }

        public String getDocumentId() {
            return tenantId + ":" + metricName;
        }

        @Override
        public String toString() {
            return "ElasticMetricDiscovery [tenantId=" + tenantId + ", metricName=" + metricName + ", annotation="
                    + annotation.toString() + "]";
        }

        public Discovery withAnnotation(Map<String, Object> annotation) {
            this.annotation = annotation;
            return this;
        }

        private XContentBuilder createSourceContent() throws IOException {
            XContentBuilder json;

            json = XContentFactory.jsonBuilder().startObject()
                    .field(TENANT_ID.toString(), tenantId)
                    .field(METRIC_NAME.toString(), metricName);


            for (Map.Entry<String, Object> entry : annotation.entrySet()) {
                json = json.field(entry.getKey(), entry.getValue());
            }
            json = json.endObject();
            return json;
        }
    }

    public static class Result {
        private final String metricName;
        private final String unit;
        private final String tenantId;

        public Result(String tenantId, String name, String unit) {
            this.tenantId = tenantId;
            this.metricName = name;
            this.unit = unit;
        }

        public String getTenantId() {
            return tenantId;
        }
        public String getMetricName() {
            return metricName;
        }
        public String getUnit() {
            return unit;
        }
        @Override
        public String toString() {
            return "Result [tenantId=" + tenantId + ", metricName=" + metricName + ", unit=" + unit + "]";
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

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null) {
                return false;
            } else if (!getClass().equals(obj.getClass())) {
                return false;
            }
            return equals((Result) obj);
        }
        public boolean equals(Result other) {
            if (this == other) {
                return true;
            }
            return metricName.equals(other.metricName) && unit.equals(other.unit) && tenantId.equals(other.tenantId);
        }
    }
}
