package com.rackspacecloud.blueflood;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.datastax.DCassandraUtilsIO;
import com.rackspacecloud.blueflood.utils.CassandraTestServer;
import com.rackspacecloud.blueflood.utils.ElasticsearchTestServer;
import com.rackspacecloud.blueflood.utils.TestServers;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

//@Ignore("because this is a wip")
public class IngestQueryIntegrationTest extends HttpIntegrationTestBase {
    // Can't extend this because it causes the db code to initialize, and the db may not be up yet.
//    private static HttpIntegrationTestBase httpIntegrationTestBase;
    private ObjectMapper mapper = new ObjectMapper();

//    @BeforeClass
//    public static void startElasticsearch() throws Exception {
//        TestServers.ensureStarted();
//        HttpIntegrationTestBase.setUpHttp();
//        httpIntegrationTestBase = new HttpIntegrationTestBase();
//    }

//    @AfterClass
//    public static void stopThings() throws Exception {
//        HttpIntegrationTestBase.shutdown();
//    }

    @Test
    public void ingestAndRetrieveOneMetric() throws Exception {
        long start = System.currentTimeMillis();
        Map<String, Object> metric = new HashMap<String, Object>() {{
            put("metricName", "com.example.metric.one.banana");
            put("unit", "monkeys");
            put("collectionTime", start);
            put("metricValue", 13579);
            put("ttlInSeconds", 10);
        }};
        List<Map<String, Object>> metricList = Collections.singletonList(metric);
        String json = mapper.writeValueAsString(metricList);

        HttpResponse createResponse = httpPost("13579", "/v2.0/13579/ingest", json);
        EntityUtils.consume(createResponse.getEntity());
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200));
        HttpResponse queryResponse = querySingleplot("13579", "com.example.metric.one.banana",
                start, System.currentTimeMillis(), "", "full", "");
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200));
        Map<?, ?> retrievedData = mapper.readValue(EntityUtils.toString(queryResponse.getEntity()), Map.class);

        assertThat(retrievedData, equalTo(new HashMap<String, Object>() {{
            put("metadata", new HashMap<String, Object>() {{
                put("marker", null);
                put("count", 1);
                put("limit", null);
                put("next_href", null);
            }});
            put("unit", "monkeys");
            put("values", Collections.singletonList(new HashMap<String, Object>() {{
                put("numPoints", 1);
                put("average", 13579);
                put("timestamp", start);
            }}));
        }}));
    }

    @Test
    public void ingestAndRetrieveOneTaggedMetric() throws Exception {
        long start = System.currentTimeMillis();
        Map<String, Object> metric = new HashMap<String, Object>() {{
            put("metricName", "bananas");
            put("unit", "bunch");
            put("collectionTime", start);
            put("metricValue", 24680);
            put("ttlInSeconds", 10);
            put("tags", new HashMap<String, String>() {{
                put("name", "bob");
                put("pet", "monkey");
            }});
        }};
        List<Map<String, Object>> metricList = Collections.singletonList(metric);
        String json = mapper.writeValueAsString(metricList);

        HttpResponse createResponse = httpPost("13579", "/v2.0/13579/ingest", json);
        EntityUtils.consume(createResponse.getEntity());
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200));
//        pause("where is the tagged metric now?");
        // todo: this is the wrong query. can't query by name for tagged metrics. have to know the metric uid!
        HttpResponse queryResponse = querySingleplot("13579", "bananas",
                start, System.currentTimeMillis(), "", "full", "");
        assertThat(createResponse.getStatusLine().getStatusCode(), equalTo(200));
        Map<?, ?> retrievedData = mapper.readValue(EntityUtils.toString(queryResponse.getEntity()), Map.class);

        assertThat(retrievedData, equalTo(new HashMap<String, Object>() {{
            put("metadata", new HashMap<String, Object>() {{
                put("marker", null);
                put("count", 1);
                put("limit", null);
                put("next_href", null);
            }});
            put("unit", "bunch");
            put("values", Collections.singletonList(new HashMap<String, Object>() {{
                put("numPoints", 1);
                put("average", 24680);
                put("timestamp", start);
            }}));
        }}));
    }
}
