/*
 * Copyright 2015 Rackspace
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
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class EnumElasticIO extends AbstractElasticIO {

    public static final String ENUMS_DOCUMENT_TYPE = "metrics";

    public EnumElasticIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public EnumElasticIO(Client client) {
        this.client = client;
    }

    public EnumElasticIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    public void insertDiscovery(IMetric metric) throws IOException {
        List<IMetric> batch = new ArrayList<IMetric>();
        batch.add(metric);
        insertDiscovery(batch);
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
                if (!(obj instanceof PreaggregatedMetric)) {
                    classCastExceptionMeter.mark();
                    continue;
                }

                // get PreaggregatedMetric
                PreaggregatedMetric metric = (PreaggregatedMetric)obj;

                // get rollup object of metric values
                BluefloodEnumRollup rollup = (BluefloodEnumRollup) metric.getMetricValue();
                if (rollup == null) {
                    continue;
                }

                Locator locator = metric.getLocator();
                Discovery discovery = new Discovery(locator.getTenantId(), locator.getMetricName());

                Map<String, Object> fields = new HashMap<String, Object>();

                ArrayList<String> enumValues = rollup.getStringEnumValues();
                if (enumValues.size() > 0) {
                    fields.put(ESFieldLabel.enum_values.toString(), enumValues);
                }

                discovery.withSourceFields(fields);
                bulk.add(createSingleRequest(discovery));
            }
            bulk.execute().actionGet();
        } finally {
            ctx.stop();
        }
    }

    IndexRequestBuilder createSingleRequest(Discovery metricDiscovery) throws IOException {
        if (metricDiscovery.getMetricName() == null) {
            throw new IllegalArgumentException("trying to insert enum metric discovery without a metricName");
        }
        return client.prepareIndex(ENUMS_INDEX_NAME_WRITE, ENUMS_DOCUMENT_TYPE)
                .setId(metricDiscovery.getDocumentId())
                .setSource(metricDiscovery.createSourceContent())
                .setRouting(metricDiscovery.getTenantId());
    }

    @VisibleForTesting
    public void setINDEX_NAME_WRITE (String indexNameWrite) {
        ENUMS_INDEX_NAME_WRITE = indexNameWrite;
    }

    @VisibleForTesting
    public void setINDEX_NAME_READ (String indexNameRead) {
        ENUMS_INDEX_NAME_READ = indexNameRead;
    }

    @Override
    protected String[] getIndexesToSearch() {
        return new String[] {ENUMS_INDEX_NAME_READ, ELASTICSEARCH_INDEX_NAME_READ};
    }

    @Override
    protected SearchResult convertHitToMetricDiscoveryResult(SearchHit hit) {

        SearchResult result;
        Map<String, Object> source = hit.getSource();

        String metricName = (String)source.get(ESFieldLabel.metric_name.toString());
        String tenantId = (String)source.get(ESFieldLabel.tenantId.toString());
        String unit = (String)source.get(ESFieldLabel.unit.toString());

        if (source.containsKey(ESFieldLabel.enum_values.toString())) {
            ArrayList<String> enumValues = (ArrayList<String>)source.get(ESFieldLabel.enum_values.toString());
            result = new SearchResult(tenantId, metricName, unit, enumValues);
        }
        else {
            result = new SearchResult(tenantId, metricName, unit);
        }

        return result;
    }

    @Override
    protected  List<SearchResult> dedupResults(List<SearchResult> results) {
        HashMap<String, SearchResult> dedupedResults = new HashMap<String, SearchResult>();
        for (SearchResult result : results) {
            //check if result has enum_values
            if (result.getEnumValues() != null) {
                // if it does, always put
                dedupedResults.put(result.getMetricName(), result);
            } else {
                // if it doesn't have enum_values, put only if it doesn't already exists
                if (!dedupedResults.containsKey(result.getMetricName())) {
                    dedupedResults.put(result.getMetricName(), result);
                }
            }
        }
        return Lists.newArrayList(dedupedResults.values());
    }


    @VisibleForTesting
    public void setClient(Client client) {
        this.client = client;
    }

}
