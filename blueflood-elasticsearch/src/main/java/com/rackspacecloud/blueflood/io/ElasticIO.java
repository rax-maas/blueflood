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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.types.IMetric;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ElasticIO extends AbstractElasticIO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticIO.class);

    public ElasticIO() {
        this.elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
    }

    public ElasticIO(Client client) {
        this.client = client;
    }

    /**
     * Insert single metric document.
     * @param metric
     * @throws IOException
     */
    public void insertDiscovery(IMetric metric) throws IOException {
        //TODO: why there is no input validation for metric parameter? What will happen when it's null?
        List<IMetric> batch = new ArrayList<>();
        batch.add(metric);
        insertDiscovery(batch);
    }

    // REST call to index into ES
    public void insertDiscovery(List<IMetric> batch) {
        batchHistogram.update(batch.size());
        if (batch.size() == 0) {
            return;
        }

        Timer.Context ctx = writeTimer.time();
        try {
            for (Object obj : batch) {
                if (!(obj instanceof IMetric)) {
                    classCastExceptionMeter.mark();
                    continue;
                }
            }
            elasticsearchRestHelper.indexMetrics(batch);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            ctx.stop();
        }
    }

    @VisibleForTesting
    public void setINDEX_NAME_WRITE (String indexNameWrite) {
        ELASTICSEARCH_INDEX_NAME_WRITE = indexNameWrite;
    }

    @VisibleForTesting
    public void setINDEX_NAME_READ (String indexNameRead) {
        ELASTICSEARCH_INDEX_NAME_READ = indexNameRead;
    }

    @Override
    protected String[] getIndexesToSearch() {
        return new String[] {ELASTICSEARCH_INDEX_NAME_READ};
    }

    @Override
    protected List<SearchResult> dedupResults(List<SearchResult> results) {
        HashMap<String, SearchResult> dedupedResults = new HashMap<>();
        for (SearchResult result : results)
            dedupedResults.put(result.getMetricName(), result);
        return Lists.newArrayList(dedupedResults.values());
    }

    @VisibleForTesting
    public void setClient(Client client) {
        this.client = client;
    }
}
