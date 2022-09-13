package com.rackspacecloud.blueflood.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.utils.ElasticsearchTestServer;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.IndicesQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class CanIUseSpringDataIntegrationTest {
    protected ElasticIO elasticIO;
    protected ElasticTokensIO elasticTokensIO;
    private ElasticsearchOperations elasticsearchOperations;

    @BeforeClass
    public static void startElasticsearch() {
        ElasticsearchTestServer.getInstance().ensureStarted();
    }

    @Test
    public void stuff() throws IOException, InterruptedException {
        Settings settings = ImmutableSettings.builder()
                // What kind of settings are there?
                .build();
//        int port = ElasticsearchTestServer.getInstance().getElasticsearchTransportPort();
        TransportClient client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        ObjectMapper objectMapper = new ObjectMapper();
        elasticsearchOperations = new ElasticsearchTemplate(client);
        elasticsearchOperations.createIndex("metric-tagged");
//        elasticsearchOperations.putMapping("metric-tagged", "metric",
//                "{\"dynamic\": false, \"properties\": {\"locator\": {\"type\": \"string\"}, \"tags\": {\"type\": \"nested\", \"properties\": {\"key\": {\"type\": \"string\"}, \"value\": {\"type\": \"string\"}}}}}");
        String mapping = objectMapper.writeValueAsString(new HashMap<String, Object>() {{
            put("dynamic", false);
            put("properties", new HashMap<String, Object>() {{
                put("locator", new HashMap<String, Object>() {{
                    put("type", "string");
                }});
                put("tags", new HashMap<String, Object>() {{
                    put("type", "nested");
                    put("properties", new HashMap<String, Object>() {{
                        put("key", new HashMap<String, Object>() {{
                            put("type", "string");
                        }});
                        put("value", new HashMap<String, Object>() {{
                            put("type", "string");
                        }});
                    }});
                }});
            }});
        }});
        elasticsearchOperations.putMapping("metric-tagged", "metric", mapping);
        System.out.println(elasticsearchOperations.getMapping("metric-tagged", "metric"));
        String source = objectMapper.writeValueAsString(new HashMap<String, Object>() {{
            put("locator", "bob-1234");
            put("tags", Arrays.asList(
                    new HashMap<String, String>() {{
                        put("key", "dc");
                        put("value", "ord1");
                    }},
                    new HashMap<String, String>() {{
                        put("key", "kind");
                        put("value", "web");
                    }}
            ));
        }});
        elasticsearchOperations.index(new IndexQueryBuilder()
                .withIndexName("metric-tagged")
                .withType("metric")
                .withId("42")
                .withSource(source)
                .build());
        Thread.sleep(1000);
//        SearchQuery query = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchAllQuery()).build();
//        query.addIndices("metric-tags");
//        query.addTypes("metric");
        SearchQuery query = new NativeSearchQuery(
                new IndicesQueryBuilder(QueryBuilders.matchAllQuery(), "metric-tags"));
        System.out.println(elasticsearchOperations.query(query, (ResultsExtractor<Void>) response -> {
            System.out.println(response);
            return null;
        }));
    }
}
