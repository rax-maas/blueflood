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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.GlobPattern;
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
import java.util.*;

import static com.rackspacecloud.blueflood.io.ElasticIO.ESFieldLabel.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticIO implements DiscoveryIO {
    public static String INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_WRITE);
    public static String INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_INDEX_NAME_READ);
    
    static enum ESFieldLabel {
        metric_name,
        tenantId,
        unit
    }
    
    private static final Logger log = LoggerFactory.getLogger(DiscoveryIO.class);;
    private static final String ES_TYPE = "metrics";
    private Client client;
    
    // todo: these should be instances per client.
    private final Timer searchTimer = Metrics.timer(ElasticIO.class, "Search Duration");
    private final Timer writeTimer = Metrics.timer(ElasticIO.class, "Write Duration");
    private final Histogram batchHistogram = Metrics.histogram(ElasticIO.class, "Batch Sizes");
    private Meter classCastExceptionMeter = Metrics.meter(ElasticIO.class, "Failed Cast to IMetric");
    private Histogram queryBatchHistogram = Metrics.histogram(ElasticIO.class, "Query Batch Size");

    public ElasticIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public ElasticIO(Client client) {
        this.client = client;
    }

    public ElasticIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    private static SearchResult convertHitToMetricDiscoveryResult(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        String metricName = (String)source.get(metric_name.toString());
        String tenantId = (String)source.get(ESFieldLabel.tenantId.toString());
        String unit = (String)source.get(ESFieldLabel.unit.toString());
        SearchResult result = new SearchResult(tenantId, metricName, unit);

        return result;
    }

    public void insertDiscovery(List<IMetric> batch) throws IOException {
        batchHistogram.update(batch.size());
        if (batch.size() == 0) {
            return;
        }
        
        // TODO: check bulk insert result and retry
        Timer.Context ctx = writeTimer.time();
        try {
            BulkRequestBuilder bulk = client.prepareBulk();
            for (Object obj : batch) {
                if (!(obj instanceof IMetric)) {
                    classCastExceptionMeter.mark();
                    continue;
                }

                IMetric metric = (IMetric)obj;
                Locator locator = metric.getLocator();
                Discovery md = new Discovery(locator.getTenantId(), locator.getMetricName());

                Map<String, Object> info = new HashMap<String, Object>();


                if (obj instanceof  Metric && getUnit((Metric)metric) != null) { // metric units may be null
                    info.put(unit.toString(), getUnit((Metric)metric));
                }

                md.withAnnotation(info);
                bulk.add(createSingleRequest(md));
            }
            bulk.execute().actionGet();
        } finally {
            ctx.stop();
        }
    }

    private static String getUnit(Metric metric) {
        return metric.getUnit();
    }

    private IndexRequestBuilder createSingleRequest(Discovery md) throws IOException {
        if (md.getMetricName() == null) {
            throw new IllegalArgumentException("trying to insert metric discovery without a metricName");
        }
        return client.prepareIndex(INDEX_NAME_WRITE, ES_TYPE)
                .setId(md.getDocumentId())
                .setSource(md.createSourceContent())
                .setCreate(true)
                .setRouting(md.getTenantId());
    }

    @VisibleForTesting
    public void setINDEX_NAME_WRITE (String indexNameWrite) {
        INDEX_NAME_WRITE = indexNameWrite;
    }

    @VisibleForTesting
    public void setINDEX_NAME_READ (String indexNameRead) {
        INDEX_NAME_READ = indexNameRead;
    }
    
    public List<SearchResult> search(String tenant, String query) throws Exception {
        return search(tenant, Arrays.asList(query));
    }

    public List<SearchResult> search(String tenant, List<String> queries) throws Exception {
        List<SearchResult> results = new ArrayList<SearchResult>();
        Timer.Context multiSearchCtx = searchTimer.time();
        queryBatchHistogram.update(queries.size());
        BoolQueryBuilder bqb = boolQuery();
        QueryBuilder qb;

        for (String query : queries) {
            GlobPattern pattern = new GlobPattern(query);
            if (!pattern.hasWildcard()) {
                qb = termQuery(metric_name.name(), query);
            } else {
                qb = regexpQuery(metric_name.name(), pattern.compiled().toString());
            }
            bqb.should(boolQuery()
                     .must(termQuery(tenantId.toString(), tenant))
                     .must(qb)
            );
        }

        SearchResponse response = client.prepareSearch(INDEX_NAME_READ)
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

    private List<SearchResult> dedupResults(List<SearchResult> results) {
        HashMap<String, SearchResult> dedupedResults = new HashMap<String, SearchResult>();
        for (SearchResult result : results)
            dedupedResults.put(result.getMetricName(), result);
        return Lists.newArrayList(dedupedResults.values());
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
                    .field(ESFieldLabel.tenantId.toString(), tenantId)
                    .field(metric_name.toString(), metricName);


            for (Map.Entry<String, Object> entry : annotation.entrySet()) {
                json = json.field(entry.getKey(), entry.getValue());
            }
            json = json.endObject();
            return json;
        }
    }

    @VisibleForTesting
    public void setClient(Client client) {
        this.client = client;
    }
}
