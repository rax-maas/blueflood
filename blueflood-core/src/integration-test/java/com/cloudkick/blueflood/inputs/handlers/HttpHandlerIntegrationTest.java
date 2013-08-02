package com.cloudkick.blueflood.inputs.handlers;

import com.cloudkick.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.service.Configuration;
import com.cloudkick.blueflood.service.ScheduleContext;
import com.cloudkick.blueflood.types.Locator;
import com.netflix.astyanax.model.ColumnList;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;

public class HttpHandlerIntegrationTest {
    private static HttpHandler httpHandler;
    private static Collection<Integer> manageShards = new HashSet<Integer>();
    private static DefaultHttpClient client;
    private static int httpPort;

    static {
        httpPort = Configuration.getIntegerProperty("HTTP_INGESTION_PORT");
        manageShards.add(1); manageShards.add(5); manageShards.add(6);
        ScheduleContext context = new ScheduleContext(System.currentTimeMillis(), manageShards);
        httpHandler = new HttpHandler(httpPort, context);
        client = new DefaultHttpClient();
    }

    @Test
    public void testHttpIngestionHappyCase() throws Exception {
        HttpPost post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity(JSONMetricsContainerTest.generateJSONMetricsData(),
                ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        // Now read the metrics back from dcass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
        final Locator locator = Locator.createLocatorFromPathComponents("ac1", "mzord.duration");
        ColumnList<Long> rollups = AstyanaxReader.getInstance().getNumericRollups(locator, Granularity.FULL, 1234567878, 1234567900);
        Assert.assertEquals(1, rollups.size());
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    @Test
    public void testBadRequests() throws Exception {
        HttpPost post = new HttpPost(getMetricsURI());
        HttpResponse response = client.execute(post);  // no body
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 400);
        EntityUtils.consume(response.getEntity()); // Releases connection apparently

        post = new HttpPost(getMetricsURI());
        HttpEntity entity = new StringEntity("Some incompatible json body", ContentType.APPLICATION_JSON);
        post.setEntity(entity);
        response = client.execute(post);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 400);
        EntityUtils.consume(response.getEntity()); // Releases connection apparently
    }

    private URI getMetricsURI() throws URISyntaxException {
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1")
                                             .setPort(httpPort).setPath("/metrics");
        return builder.build();
    }
}
