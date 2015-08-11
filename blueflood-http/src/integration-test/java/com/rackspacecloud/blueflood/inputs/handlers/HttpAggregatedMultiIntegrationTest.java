package com.rackspacecloud.blueflood.inputs.handlers;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.http.HttpClientVendor;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class HttpAggregatedMultiIntegrationTest {
    private static HttpIngestionService httpIngestionService;
    private static HttpClientVendor vendor;
    private static DefaultHttpClient client;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static int httpPort;
    private static ScheduleContext context;

    @BeforeClass
    public static void setUp() throws Exception{
        httpPort = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_INGESTION_PORT);
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        context = spy(new ScheduleContext(System.currentTimeMillis(), manageShards));
        HttpMetricsIngestionServer server = new HttpMetricsIngestionServer(context, new AstyanaxMetricsWriter());

        httpIngestionService = new HttpIngestionService();
        httpIngestionService.setMetricsIngestionServer(server);
        httpIngestionService.startService(context, new AstyanaxMetricsWriter());

        vendor = new HttpClientVendor();
        client = vendor.getClient();
    }

    @Test
    public void testHttpIngestionHappyCase() throws Exception {
        HttpPost post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity(generateAggregatedJsonData(),
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());

        final Locator locator = Locator.createLocatorFromPathComponents("5405532", "G200ms");
        Points<GaugeRollup> points = AstyanaxReader.getInstance().getDataToRoll(GaugeRollup.class,
                locator, new Range(1439231323000L, 1439231325000L), CassandraModel.getColumnFamily(GaugeRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points.getPoints().size());

        final Locator locator1 = Locator.createLocatorFromPathComponents("5405577", "internal.bad_lines_seen");
        Points<CounterRollup> points1 = AstyanaxReader.getInstance().getDataToRoll(CounterRollup.class,
                locator1, new Range(1439231323000L, 1439231325000L), CassandraModel.getColumnFamily(CounterRollup.class, Granularity.FULL));
        Assert.assertEquals(1, points1.getPoints().size());

        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @AfterClass
    public static void shutdown() {
        vendor.shutdown();
    }

    private URI getMetricsURI() throws URISyntaxException {
        return getMetricsURIBuilder().build();
    }

    private URIBuilder getMetricsURIBuilder() throws URISyntaxException {
        return new URIBuilder().setScheme("http").setHost("127.0.0.1")
                .setPort(httpPort).setPath("/v2.0/acTEST/ingest/aggregated/multi");
    }

    public String generateAggregatedJsonData() throws Exception{
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_multi_bundle.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();

        return json;
    }

}
