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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class EventElasticSearchIO implements EventsIO {
    private final Timer eventSearchTimer = Metrics.timer(EventElasticSearchIO.class,
            "Search time for events");
    private final Timer eventInsertTimer = Metrics.timer(EventElasticSearchIO.class,
            "Insertion time for events");
    public static final String EVENT_INDEX = "events";
    public static final String ES_TYPE = "graphite_event";
    private final Client client;

    private ElasticsearchRestHelper elasticsearchRestHelper;

    public EventElasticSearchIO() {
        this.client = null;
        this.elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
    }

//    public EventElasticSearchIO() {
//        this(RemoteElasticSearchServer.getInstance());
//    }
    public EventElasticSearchIO(Client client) {
        this.client = client;
    }
    public EventElasticSearchIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    @Override
    public void insert(String tenantId, Map<String, Object> event) throws Exception {
        final Timer.Context eventInsertTimerContext = eventInsertTimer.time();
        event.put(Event.FieldLabels.tenantId.toString(), tenantId);

        elasticsearchRestHelper.indexEvent(event);
        eventInsertTimerContext.stop();
    }

//    @Override
//    public void insert(String tenant, List<Map<String, Object>> events) throws Exception {
//        final Timer.Context eventInsertTimerContext = eventInsertTimer.time();
//        BulkRequestBuilder bulk = client.prepareBulk();
//
//        for (Map<String, Object> event : events) {
//            event.put(Event.FieldLabels.tenantId.toString(), tenant);
//            IndexRequestBuilder requestBuilder = client.prepareIndex(EVENT_INDEX, ES_TYPE)
//                    .setSource(event)
//                    .setRouting(tenant);
//            bulk.add(requestBuilder);
//        }
//        bulk.execute().actionGet();
//        eventInsertTimerContext.stop();
//    }

    @Override
    public List<Map<String, Object>> search(String tenant, Map<String, List<String>> query) throws Exception {
        final Timer.Context eventSearchTimerContext = eventSearchTimer.time();
        String result = elasticsearchRestHelper.fetchEvents(tenant, query);
        eventSearchTimerContext.stop();

        return getEventResults(result);
    }

    private List<Map<String, Object>> getEventResults(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response);

        List<Map<String, Object>> eventResults = new ArrayList<>();

        Iterator<JsonNode> iter = root.get("hits").get("hits").elements();
        while(iter.hasNext()){
            JsonNode source = iter.next().get("_source");

            Map<String, Object> map = new HashMap<>();
            if(source.has("what")) map.put("what", source.get("what").asText());
            if(source.has("when")) map.put("when", source.get("when").asLong());
            if(source.has("data")) map.put("data", source.get("data").asText());
            if(source.has("tenantId")) map.put("tenantId", source.get("tenantId").asText());
            if(source.has("tags")) map.put("tags", source.get("tags").asText());

            eventResults.add(map);
        }
        return eventResults;
    }

//    @Override
//    public List<Map<String, Object>> search(String tenant, Map<String, List<String>> query) throws Exception {
//        final Timer.Context eventSearchTimerContext = eventSearchTimer.time();
//        BoolQueryBuilder qb = boolQuery()
//                .must(termQuery(Event.FieldLabels.tenantId.toString(), tenant));
//
//        if (query != null) {
//            qb = extractQueryParameters(query, qb);
//        }
//
//        SearchResponse response = client.prepareSearch(EVENT_INDEX)
//                .setRouting(tenant)
//                .setSize(100000)
//                .setVersion(true)
//                .setQuery(qb)
//                .execute()
//                .actionGet();
//
//        eventSearchTimerContext.stop();
//
//        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
//        for (SearchHit hit : response.getHits().getHits()) {
//            events.add(hit.getSource());
//        }
//
//        return events;
//    }

    private BoolQueryBuilder extractQueryParameters(Map<String, List<String>> query, BoolQueryBuilder qb) {
        String tagsQuery = extractFieldFromQuery(Event.FieldLabels.tags.toString(), query);
        String untilQuery = extractFieldFromQuery(Event.untilParameterName, query);
        String fromQuery = extractFieldFromQuery(Event.fromParameterName, query);

        if (!tagsQuery.equals(""))
            qb = qb.must(termQuery(Event.FieldLabels.tags.toString(), tagsQuery));

        if (!untilQuery.equals("") && !fromQuery.equals("")) {
            qb = qb.must(rangeQuery(Event.FieldLabels.when.toString())
                    .to(Long.parseLong(untilQuery))
                    .from(Long.parseLong(fromQuery)));
        } else if (!untilQuery.equals("")) {
            qb = qb.must(rangeQuery(Event.FieldLabels.when.toString()).to(Long.parseLong(untilQuery)));
        } else if (!fromQuery.equals("")) {
            qb = qb.must(rangeQuery(Event.FieldLabels.when.toString()).from(Long.parseLong(fromQuery)));
        }

        return qb;
    }

    private String extractFieldFromQuery(String name, Map<String, List<String>> query) {
        String result = "";
        if (query.containsKey(name)) {
            try {
                result = query.get(name).get(0);
            }
            catch (IndexOutOfBoundsException e) {
                result = "";
            }
        }
        return result;
    }
}
