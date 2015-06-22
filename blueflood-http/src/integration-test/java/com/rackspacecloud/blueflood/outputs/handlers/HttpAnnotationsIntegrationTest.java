package com.rackspacecloud.blueflood.outputs.handlers;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.service.HttpQueryService;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class HttpAnnotationsIntegrationTest {
    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;
    private final String tenantId = "540123";
    private static int queryPort;
    private static EventElasticSearchIO eventsIO;
    private static EsSetup esSetup;
    private static HttpQueryService httpQueryService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;

    @BeforeClass
    public static void setUpHttp() {
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "com.rackspacecloud.blueflood.io.EventElasticSearchIO");
        Configuration.getInstance().setProperty(CoreConfig.USE_ES_FOR_UNITS.name(), "true");
        queryPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_METRIC_DATA_QUERY_PORT);

        vendor = new HttpClientVendor();
        client = vendor.getClient();

        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll());
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("annotations", EsSetup.fromClassPath("events_mapping.json")));
        eventsIO = new EventElasticSearchIO(esSetup.client());

        httpQueryService = new HttpQueryService();
        HttpMetricDataQueryServer server = new HttpMetricDataQueryServer();
        server.setEventsIO(eventsIO);
        httpQueryService.setServer(server);
        httpQueryService.startService();
    }

    @Before
    public void setup() throws Exception {
        createTestEvents(tenantId, 5);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void TestHttpAnnotationsHappyCase() throws Exception {
        HttpGet get = new HttpGet(getAnnotationsQueryURI());
        HttpResponse response = client.execute(get);
        String responseString = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        Assert.assertTrue(!responseString.equals("[]"));
    }

    private URI getAnnotationsQueryURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(queryPort).setPath("/v2.0/" + tenantId + "/events/get_data")
                .setParameter(Event.fromParameterName, String.valueOf(baseMillis - 86400000))
                .setParameter(Event.untilParameterName,String.valueOf(baseMillis + (86400000*3)));
        return builder.build();
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        Configuration.getInstance().setProperty(CoreConfig.EVENTS_MODULES.name(), "");
        esSetup.terminate();
        httpQueryService.stopService();
    }


    private static void createTestEvents(final String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat(String.format("[%s] %s %d", tenant, "Event title sample", i));
            event.setWhen(Long.parseLong(String.valueOf(Calendar.getInstance().getTimeInMillis())));
            event.setData(String.format("[%s] %s %d", tenant, "Event data sample", i));
            event.setTags(String.format("[%s] %s %d",tenant,"Sample tag",i));

            eventList.add(event.toMap());
        }
        eventsIO.insert(tenant, eventList);
    }
}