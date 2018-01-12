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
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.types.Event;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;

public class EventElasticSearchIO implements EventsIO {
    private final Timer eventSearchTimer = Metrics.timer(EventElasticSearchIO.class,
            "Search time for events");
    private final Timer eventInsertTimer = Metrics.timer(EventElasticSearchIO.class,
            "Insertion time for events");
    public static final String EVENT_INDEX = "events";
    public static final String ES_TYPE = "graphite_event";
    private Client client;
    private ElasticsearchRestHelper elasticsearchRestHelper;

    public EventElasticSearchIO() {
        this.elasticsearchRestHelper = ElasticsearchRestHelper.getInstance();
    }

    @Override
    public void insert(String tenantId, Map<String, Object> event) throws Exception {
        final Timer.Context eventInsertTimerContext = eventInsertTimer.time();
        event.put(Event.FieldLabels.tenantId.toString(), tenantId);

        elasticsearchRestHelper.indexEvent(event);
        eventInsertTimerContext.stop();
    }

    @Override
    public List<Map<String, Object>> search(String tenant, Map<String, List<String>> query) throws Exception {
        final Timer.Context eventSearchTimerContext = eventSearchTimer.time();

        String result = elasticsearchRestHelper.fetchEvents(tenant, query);
        eventSearchTimerContext.stop();
        List<Map<String, Object>> events = getEventResults(result);

        return events;
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

    @VisibleForTesting
    public void setClient(Client client) {
        this.client = client;
    }
}
