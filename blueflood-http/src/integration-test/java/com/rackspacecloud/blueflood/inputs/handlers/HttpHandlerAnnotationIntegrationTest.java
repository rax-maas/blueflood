package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Event;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import static com.rackspacecloud.blueflood.TestUtils.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Testing posting annotations to blueflood.
 */
public class HttpHandlerAnnotationIntegrationTest extends HttpIntegrationTestBase {

    private final String INVALID_DATA = "Invalid Data: " + ERROR_TITLE;

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
        Thread.sleep( 1200 );
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put( Event.tagsParameterName, Arrays.asList( "deployment" ));
        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );

        query = new HashMap<String, List<String>>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));
        results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );
    }

    @Test
    public void testHttpAnnotationsIngestionMultiEvents() throws Exception {
        final int batchSize = 5;
        final String tenant_id = "333444";
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
        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));
        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        assertFalse( batchSize == results.size() ); //Only saving the first event of the batch, so the result size will be 1.
        assertTrue( results.size() == 1 );

        query = new HashMap<String, List<String>>();
        query.put(Event.fromParameterName, Arrays.asList(String.valueOf(baseMillis - 86400000)));
        query.put(Event.untilParameterName, Arrays.asList(String.valueOf(baseMillis + (86400000*3))));
        results = eventsSearchIO.search(tenant_id, query);
        assertFalse( batchSize == results.size() );
        assertTrue( results.size() == 1 );
    }

    @Test
    public void testHttpAnnotationsIngestionDuplicateEvents() throws Exception {
        int batchSize = 5; // To create duplicate events
        String tenant_id = "444444";

        createAndInsertTestEvents(tenant_id, batchSize);
        esSetup.client().admin().indices().prepareRefresh().execute().actionGet();

        Map<String, List<String>> query = new HashMap<String, List<String>>();
        query.put( Event.tagsParameterName, Arrays.asList( "deployment" ) );

        List<Map<String, Object>> results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );

        query = new HashMap<String, List<String>>();
        query.put( Event.fromParameterName, Arrays.asList( String.valueOf( baseMillis - 86400000 ) ) );
        query.put( Event.untilParameterName, Arrays.asList( String.valueOf( baseMillis + ( 86400000 * 3 ) ) ) );

        results = eventsSearchIO.search(tenant_id, query);
        assertEquals( batchSize, results.size() );
    }

    @Test
    public void testHttpAnnotationIngestionInvalidPastCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent( batchSize, timestamp );
        HttpResponse response = postEvent( tenant_id, event );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( INVALID_DATA, errors[ 0 ] );
        assertTrue( Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
    }

    @Test
    public void testHttpAnnotationIngestionInvalidFutureCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 1;
        final String tenant_id = "333333";
        String event = createTestEvent( batchSize, timestamp );
        HttpResponse response = postEvent( tenant_id, event );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( INVALID_DATA, errors[ 0 ] );
        assertTrue( Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
    }

    @Test
    public void testHttpAnnotationIngestionMultiEventsInvalidPastCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 5;
        final String tenant_id = "333444";
        String event = createTestEvent(batchSize, timestamp);
        HttpResponse response = postEvent( tenant_id, event );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( INVALID_DATA, errors[ 0 ] );
        assertTrue( Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
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
        assertTrue( responseString.contains( "Invalid Data:" ) );
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
        assertTrue( responseString.contains( "Invalid Data:" ) );
    }

    @Test
    public void testHttpAnnotationIngestionMultiEventsInvalidFutureCollectionTime() throws Exception {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS  );

        final int batchSize = 5;
        final String tenant_id = "333444";
        String event = createTestEvent(batchSize, timestamp);
        HttpResponse response = postEvent( tenant_id, event );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( INVALID_DATA, errors[ 0 ] );
        assertTrue( Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
    }

    private void createAndInsertTestEvents(final String tenant, int eventCount) throws Exception {
        ArrayList<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        for (int i=0; i<eventCount; i++) {
            Event event = new Event();
            event.setWhat("deployment");
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData("deploying prod");
            event.setTags("deployment");

            eventList.add(event.toMap());
        }
        eventsSearchIO.insert(tenant, eventList);
    }
}
