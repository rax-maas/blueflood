package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Event;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;

/**
 * Testing posting annotations to blueflood.
 */
public class HttpHandlerAnnotationIntegrationTest extends HttpIntegrationTestBase {

    //A time stamp 2 days ago
    private final long baseMillis = System.currentTimeMillis() - 172800000;

    @Test
    public void testHttpAnnotationsIngestionHappyCase() throws Exception {
        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent(batchSize);
        HttpResponse response = postEvent( tenant_id, event );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }

        //Sleep for a while
        Thread.sleep(1200);
        Map<String, List<String>> query = new HashMap<>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));
        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );

        query = new HashMap<>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));
        results = eventsSearchIO.search(tenant_id, query);
        assertEquals(batchSize, results.size());
    }

    @Test
    public void testHttpAnnotationsIngestionMultiEvents() throws Exception {
        final int batchSize = 5;
        final String tenant_id = "333444";
        String event = createTestEvent(batchSize);
        HttpResponse response = postEvent( tenant_id, event );

        ErrorResponse errorResponse = getErrorResponse(response);
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Invalid Data: Only one event is allowed per request", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", tenant_id, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
    }

    @Test
    public void testHttpAnnotationsIngestionDuplicateEvents() throws Exception {
        int batchSize = 5; // To create duplicate events
        String tenant_id = "444444";

        createAndInsertTestEvents(tenant_id, batchSize);

        Map<String, List<String>> query = new HashMap<>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));

        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );

        query = new HashMap<>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000 * 3))));

        results = eventsSearchIO.search(tenant_id, query);
        assertEquals(batchSize, results.size());
    }

    @Test
    public void testHttpAnnotationIngestionInvalidPastCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent( batchSize, timestamp );
        HttpResponse response = postEvent(tenant_id, event);

        ErrorResponse errorResponse = getErrorResponse(response);
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", tenant_id, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
    }

    @Test
    public void testHttpAnnotationIngestionInvalidFutureCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent( batchSize, timestamp );
        HttpResponse response = postEvent( tenant_id, event );

        ErrorResponse errorResponse = getErrorResponse(response);
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than 259200000 milliseconds into the past." +
                " Cannot be more than 600000 milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
        assertEquals("Invalid tenant", tenant_id, errorResponse.getErrors().get(0).getTenantId());
        assertEquals("Invalid status", HttpResponseStatus.BAD_REQUEST.code(), response.getStatusLine().getStatusCode());
    }


    @Test
    public void testIngestingInvalidJAnnotationsJSON() throws Exception {

        String requestBody = //Invalid JSON with single inverted commas instead of double.
                "{'when':346550008," +
                        "'what':'Dummy Event'," +
                        "'data':'Dummy Data'," +
                        "'tags':'deployment'}";

        HttpResponse response = postEvent( "456854", requestBody );

        String responseString = EntityUtils.toString(response.getEntity());
        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertTrue( responseString.contains("Invalid Data:") );
    }

    @Test
    public void testIngestingInvalidAnnotationsData() throws Exception {

        String requestBody = //Invalid Data.
                "{\"how\":346550008," +
                        "\"why\":\"Dummy Event\"," +
                        "\"info\":\"Dummy Data\"," +
                        "\"tickets\":\"deployment\"}";

        HttpResponse response = postEvent( "456854", requestBody );

        String responseString = EntityUtils.toString(response.getEntity());
        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertTrue(responseString.contains("Invalid Data:"));
    }


    private void createAndInsertTestEvents(final String tenant, int eventCount) throws Exception {
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat("deployment");
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData("deploying prod");
            event.setTags("deployment");
            eventsSearchIO.insert(tenant, event.toMap());
        }

        ((EventElasticSearchIO)eventsSearchIO).elasticsearchRestHelper.refreshIndex(EventElasticSearchIO.EVENT_INDEX);
    }

    private ErrorResponse getErrorResponse(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(response.getEntity().getContent(), ErrorResponse.class);
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        URIBuilder builder = new URIBuilder().setScheme("http").setHost("127.0.0.1").setPort(9200).setPath("/events");
        HttpDelete delete = new HttpDelete(builder.build());
        HttpResponse response = client.execute(delete);
        if(response.getStatusLine().getStatusCode() != 200)
        {
            System.out.println("Couldn't delete 'events' index after running tests.");
        }
        else {
            System.out.println("Successfully deleted 'events' index after running tests.");
        }
    }
}
