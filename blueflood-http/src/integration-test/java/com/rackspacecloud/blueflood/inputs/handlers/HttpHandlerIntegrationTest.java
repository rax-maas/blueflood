/*
 * Copyright 2013-2015 Rackspace
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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainerTest;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class HttpHandlerIntegrationTest extends HttpIntegrationTestBase {

    static private final String TID = "acTEST";

    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    @Test
    public void testHttpIngestionHappyCase() throws Exception {

        String prefix = getPrefix();

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        HttpResponse response = postGenMetric( TID, prefix, postPath );

        String[] errors = getBodyArray( response );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            // assert that the update method on the ScheduleContext object was called and completed successfully
            // Now read the metrics back from cass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
            final Locator locator = Locator.createLocatorFromPathComponents( "acTEST", prefix + "mzord.duration" );
            Points<SimpleNumber> points = AstyanaxReader.getInstance().getDataToRoll( SimpleNumber.class,
                    locator, new Range( start, end ), CassandraModel.getColumnFamily( BasicRollup.class, Granularity.FULL ) );
            assertEquals( 1, points.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpIngestionInvalidPastCollectionTime() throws Exception {

        String prefix = "HttpIngestionInvalidPastCollectionTime";

        long time = System.currentTimeMillis() - 600000 - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postGenMetric( TID, prefix, postPath, time );

        assertEquals( 400, response.getStatusLine().getStatusCode() );

        String[] output = getBodyArray( response );

        assertTrue( Pattern.matches( JSONMetricsContainerTest.PAST_COLLECTION_TIME_REGEX, output[ 1 ] ) );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.PAST_COLLECTION_TIME_REGEX, output[ 2 ] ) );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.PAST_COLLECTION_TIME_REGEX, output[ 3 ] ) );
    }

    @Test
    public void testHttpIngestionInvalidFutureCollectionTime() throws Exception {

        String prefix = getPrefix();

        long time = System.currentTimeMillis() + 600000 + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postGenMetric( TID, prefix, postPath, time );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        String[] output = getBodyArray( response );

        assertTrue( Pattern.matches( JSONMetricsContainerTest.FUTURE_COLLECTION_TIME_REGEX, output[ 1 ] ) );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.FUTURE_COLLECTION_TIME_REGEX, output[ 2 ] ) );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.FUTURE_COLLECTION_TIME_REGEX, output[ 3 ] ) );
    }

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
        query.put(Event.tagsParameterName, Arrays.asList("deployment"));
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

    @Ignore
    @Test
    public void testHttpAnnotationIngestionInvalidPastCollectionTime() {

        assertTrue( false );
    }

    @Ignore
    @Test
    public void testHttpAnnotationIngestionInvalidFutureCollectionTime() {

        assertTrue( false );
    }

    @Ignore
    @Test
    public void testHttpMultiAnnotationIngestionInvalidPastCollectionTime() {

        assertTrue( false );
    }

    @Ignore
    @Test
    public void testHttpMultiAnnotationIngestionInvalidFutureCollectionTime() {

        assertTrue( false );
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
    public void testHttpAggregatedIngestionInvalidPastCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() - TIME_DIFF
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", timestamp, getPrefix() );

        String[] errors = getBodyArray(  response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.PAST_COLLECTION_TIME_REGEX, errors[ 1 ] ));
    }

    @Test
    public void testHttpAggregatedIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", timestamp, getPrefix() );

        String[] errors = getBodyArray(  response );

        assertEquals(  400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertTrue( Pattern.matches( JSONMetricsContainerTest.FUTURE_COLLECTION_TIME_REGEX, errors[ 1 ] ));
    }


    @Test
    public void testHttpAggregatedIngestionHappyCase() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        String prefix = getPrefix();

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", prefix );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            final Locator locator = Locator.
                    createLocatorFromPathComponents( "333333", prefix + "internal", "packets_received" );
            Points<BluefloodCounterRollup> points = AstyanaxReader.getInstance().getDataToRoll( BluefloodCounterRollup.class,
                    locator, new Range( start, end ),
                    CassandraModel.getColumnFamily( BluefloodCounterRollup.class, Granularity.FULL ) );
            assertEquals( 1, points.getPoints().size() );
        } finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedMultiIngestionHappyCase() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        String prefix = getPrefix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json", prefix );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );

            final Locator locator = Locator.createLocatorFromPathComponents( "5405532", prefix + "G200ms" );
            Points<BluefloodGaugeRollup> points = AstyanaxReader.getInstance().getDataToRoll( BluefloodGaugeRollup.class,
                    locator, new Range( start, end ), CassandraModel.getColumnFamily( BluefloodGaugeRollup.class, Granularity.FULL ) );
            assertEquals( 1, points.getPoints().size() );

            final Locator locator1 = Locator.createLocatorFromPathComponents( "5405577", prefix + "internal.bad_lines_seen" );
            Points<BluefloodCounterRollup> points1 = AstyanaxReader.getInstance().getDataToRoll( BluefloodCounterRollup.class,
                    locator1, new Range( start, end ), CassandraModel.getColumnFamily( BluefloodCounterRollup.class, Granularity.FULL ) );
            assertEquals( 1, points1.getPoints().size() );

            final Locator locator2 = Locator.createLocatorFromPathComponents( "5405577", prefix + "call_xyz_api" );
            Points<BluefloodEnumRollup> points2 = AstyanaxReader.getInstance().getDataToRoll( BluefloodEnumRollup.class,
                    locator2, new Range( start, end ), CassandraModel.getColumnFamily( BluefloodEnumRollup.class, Granularity.FULL ) );
            assertEquals( 1, points2.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedMultiIngestion_WithMultipleEnumPoints() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        String prefix = getPrefix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_enums_payload.json", prefix );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );

            final Locator locator2 = Locator.createLocatorFromPathComponents( "99988877", prefix + "call_xyz_api" );
            Points<BluefloodEnumRollup> points2 = AstyanaxReader.getInstance().getDataToRoll( BluefloodEnumRollup.class,
                    locator2, new Range( start, end ), CassandraModel.getColumnFamily( BluefloodEnumRollup.class, Granularity.FULL ) );
            assertEquals( 2, points2.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testBadRequests() throws Exception {


        HttpResponse response = httpPost( TID, postPath, "" );

        try {
            assertEquals( response.getStatusLine().getStatusCode(), 400 );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }

        response = httpPost( TID, postPath, "Some incompatible json body" );

        try {
            assertEquals( response.getStatusLine().getStatusCode(), 400 );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testCompressedRequests() throws Exception{

        URIBuilder builder = getMetricsURIBuilder()
                .setPath( "/v2.0/acTEST/ingest" );

        HttpPost post = new HttpPost( builder.build() );
        String content = JSONMetricsContainerTest.generateJSONMetricsData();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(content.length());
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        gzipOut.write(content.getBytes());
        gzipOut.close();
        ByteArrayEntity entity = new ByteArrayEntity(baos.toByteArray());
        //Setting the content encoding to gzip
        entity.setContentEncoding("gzip");
        baos.close();
        post.setEntity(entity);
        HttpResponse response = client.execute(post);

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantBatching() throws Exception{

        long start = System.currentTimeMillis() - TIME_DIFF;
        long end = System.currentTimeMillis() + TIME_DIFF;

        HttpResponse response = httpPost( TID, postMultiPath, JSONMetricsContainerTest.generateMultitenantJSONMetricsData() );

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            // assert that the update method on the ScheduleContext object was called and completed successfully
            // Now read the metrics back from cass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
            final Locator locator = Locator.createLocatorFromPathComponents( "tenantOne", "mzord.duration" );
            Points<SimpleNumber> points = AstyanaxReader.getInstance().getDataToRoll( SimpleNumber.class,
                    locator, new Range( start, end ), CassandraModel.getColumnFamily( BasicRollup.class, Granularity.FULL ) );
            assertEquals( 1, points.getPoints().size() );

            final Locator locatorTwo = Locator.createLocatorFromPathComponents( "tenantTwo", "mzord.duration" );
            Points<SimpleNumber> pointsTwo = AstyanaxReader.getInstance().getDataToRoll( SimpleNumber.class,
                    locator, new Range( start, end ), CassandraModel.getColumnFamily( BasicRollup.class, Granularity.FULL ) );
            assertEquals( 1, pointsTwo.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantFailureForSingleTenantHandler() throws Exception {

        HttpResponse response = httpPost( TID, postPath, JSONMetricsContainerTest.generateMultitenantJSONMetricsData() );
        try {
            assertEquals( 400, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantFailureWithoutTenant() throws Exception {

        HttpResponse response = postGenMetric( TID, "", postMultiPath );

        String[] output= getBodyArray( response );

        try {
            assertEquals( 400, response.getStatusLine().getStatusCode() );
            assertTrue( Pattern.matches( JSONMetricsContainerTest.NO_TENANT_ID_REGEX, output[ 1 ] ) );
            assertTrue( Pattern.matches( JSONMetricsContainerTest.NO_TENANT_ID_REGEX, output[ 2 ] ) );
            assertTrue( Pattern.matches( JSONMetricsContainerTest.NO_TENANT_ID_REGEX, output[ 3 ] ) );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
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

    private String createTestEvent(int batchSize) throws Exception {
        StringBuilder events = new StringBuilder();
        for (int i=0; i<batchSize; i++) {
            Event event = new Event();
            event.setWhat("deployment "+i);
            event.setWhen(Calendar.getInstance().getTimeInMillis());
            event.setData("deploying prod "+i);
            event.setTags("deployment "+i);
            events.append(new ObjectMapper().writeValueAsString(event));
        }
        return events.toString();
    }
}
