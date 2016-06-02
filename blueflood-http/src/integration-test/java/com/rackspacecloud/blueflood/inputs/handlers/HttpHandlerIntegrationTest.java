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
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class HttpHandlerIntegrationTest extends HttpIntegrationTestBase {

    static private final String TID = "acTEST";

    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    @Test
    public void testHttpIngestionHappyCase() throws Exception {

        String postfix = getPostfix();

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        HttpResponse response = postGenMetric( TID, postfix, postPath );
        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            // assert that the update method on the ScheduleContext object was called and completed successfully
            // Now read the metrics back from cass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
            final Locator locator = Locator.createLocatorFromPathComponents( "acTEST", "mzord.duration" + postfix );
            Points<SimpleNumber> points = metricsRW.getDataToRollup(
                    locator, RollupType.BF_BASIC, new Range(start, end),
                    CassandraModel.getBasicColumnFamilyName(Granularity.FULL));
            assertEquals( 1, points.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpIngestionInvalidPastCollectionTime() throws Exception {

        String postfix = getPostfix();

        long time = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postGenMetric( TID, postfix, postPath, time );

        assertEquals( 400, response.getStatusLine().getStatusCode() );

        String[] output = getBodyArray( response );

        assertTrue(output[ 1 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, output[ 1 ] ) );
        assertTrue(output[ 2 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, output[ 2 ] ) );
        assertTrue(output[ 3 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, output[ 3 ] ) );
    }

    @Test
    public void testHttpIngestionInvalidFutureCollectionTime() throws Exception {

        String postfix = getPostfix();

        long time = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postGenMetric( TID, postfix, postPath, time );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        String[] output = getBodyArray( response );

        assertEquals( 4, output.length );
        assertTrue( output[ 1 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, output[ 1 ] ) );
        assertTrue( output[ 2 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, output[ 2 ] ) );
        assertTrue( output[ 3 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, output[ 3 ] ) );
    }

    @Test
    public void testHttpIngestionPartialInvalidCollectionTime() throws Exception {
        String postfix = getPostfix();
        long validTime = System.currentTimeMillis();
        long pastTime = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
        long futureTime = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        String jsonBody = getJsonFromFile("sample_multi_payload_with_different_time.json", postfix);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_1%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_2%\"", pastTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_3%\"", futureTime);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_.%", TID);

        HttpResponse response = httpPost( TID, postMultiPath, jsonBody );
        String[] output = getBodyArray( response );

        try {
            assertEquals("Should get status 207 from " + String.format(postMultiPath, TID), 207, response.getStatusLine().getStatusCode() );
            assertEquals("", output[0]);
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedIngestionInvalidPastCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", timestamp, getPostfix() );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( ERROR_TITLE, errors[ 0 ] );
        assertTrue( errors[ 1 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 1 ] ));
    }

    @Test
    public void testHttpAggregatedIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", timestamp, getPostfix() );

        String[] errors = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals( 2, errors.length );
        assertEquals( ERROR_TITLE, errors[ 0 ] );
        assertTrue( errors[ 1 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
    }

    @Test
    public void testHttpAggregatedIngestionHappyCase() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedPath, "sample_payload.json", postfix );

        MetricsRW metricsRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            final Locator locator = Locator.
                    createLocatorFromPathComponents( "333333", "internal", "packets_received" + postfix );
            Points<BluefloodCounterRollup> points =metricsRW.getDataToRollup(
                    locator, RollupType.COUNTER, new Range( start, end ),
                    CassandraModel.getPreaggregatedColumnFamilyName(Granularity.FULL) );
            assertEquals( 1, points.getPoints().size() );
        } finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedMultiIngestionInvalidPastCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS
                - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json",
                timestamp,
                postfix );

        String errors[] = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );

        assertEquals( 4, errors.length );
        assertEquals( ERROR_TITLE, errors[ 0 ] );
        assertTrue( errors[ 1 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
        assertTrue( errors[ 2 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 2 ] ) );
        assertTrue( errors[ 3 ] + " did not match past pattern " + PAST_COLLECTION_TIME_REGEX, Pattern.matches( PAST_COLLECTION_TIME_REGEX, errors[ 3 ] ) );
    }


    @Test
    public void testHttpAggregatedMultiIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS
                + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json",
                timestamp,
                postfix );

        String errors[] = getBodyArray( response );

        assertEquals( 400, response.getStatusLine().getStatusCode() );

        assertEquals( 4, errors.length );
        assertEquals( ERROR_TITLE, errors[ 0 ] );
        assertTrue( errors[ 1 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 1 ] ) );
        assertTrue( errors[ 2 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 2 ] ) );
        assertTrue( errors[ 3 ] + " did not match future pattern " + FUTURE_COLLECTION_TIME_REGEX, Pattern.matches( FUTURE_COLLECTION_TIME_REGEX, errors[ 3 ] ) );
    }

    @Test
    public void testHttpAggregatedMultiIngestionHappyCase() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json", postfix );

        MetricsRW metricsRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );

            final Locator locator = Locator.createLocatorFromPathComponents( "5405532", "G200ms" + postfix );
            Points<BluefloodGaugeRollup> points = metricsRW.getDataToRollup(
                    locator, RollupType.GAUGE, new Range(start, end),
                    CassandraModel.getPreaggregatedColumnFamilyName(Granularity.FULL));
            assertEquals( 1, points.getPoints().size() );

            final Locator locator1 = Locator.createLocatorFromPathComponents( "5405577", "internal.bad_lines_seen" + postfix );
            Points<BluefloodCounterRollup> points1 = metricsRW.getDataToRollup(
                    locator1, RollupType.COUNTER, new Range(start, end),
                    CassandraModel.getPreaggregatedColumnFamilyName(Granularity.FULL));
            assertEquals( 1, points1.getPoints().size() );

            final Locator locator2 = Locator.createLocatorFromPathComponents( "5405577", "call_xyz_api" + postfix );
            Points<BluefloodEnumRollup> points2 = metricsRW.getDataToRollup(
                    locator2, RollupType.ENUM, new Range(start, end),
                    CassandraModel.getPreaggregatedColumnFamilyName(Granularity.FULL));
            assertEquals( 1, points2.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedMultiPartialInvalidCollectionTime() throws Exception {
        String postfix = getPostfix();
        long validTime = System.currentTimeMillis();
        long pastTime = System.currentTimeMillis() - TIME_DIFF_MS - Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
        long futureTime = System.currentTimeMillis() + TIME_DIFF_MS + Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

        String jsonBody = getJsonFromFile("sample_multi_aggregated_payload_with_different_time.json", postfix);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_1%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_2%\"", pastTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_3%\"", futureTime);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_.%", TID);

        HttpResponse response = httpPost( TID, postAggregatedMultiPath, jsonBody );
        String[] output = getBodyArray( response );

        try {
            assertEquals("Should get status 207 from " + String.format(postAggregatedMultiPath, TID), 207, response.getStatusLine().getStatusCode() );
            assertEquals("", output[0]);
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedMultiIngestion_WithMultipleEnumPoints() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_enums_payload.json", postfix );

        MetricsRW metricsRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );

            final Locator locator2 = Locator.createLocatorFromPathComponents( "99988877", "call_xyz_api" + postfix );
            Points<BluefloodEnumRollup> points2 = metricsRW.getDataToRollup(
                    locator2, RollupType.ENUM, new Range(start, end),
                    CassandraModel.getPreaggregatedColumnFamilyName(Granularity.FULL));
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
            assertEquals( 400, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }

        response = httpPost( TID, postPath, "Some incompatible json body" );

        try {
            assertEquals( 400, response.getStatusLine().getStatusCode() );
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
        String content = generateJSONMetricsData();
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

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        HttpResponse response = httpPost( TID, postMultiPath, generateMultitenantJSONMetricsData() );

        MetricsRW metricsRW = IOContainer.fromConfig().getBasicMetricsRW();

        try {
            assertEquals( 200, response.getStatusLine().getStatusCode() );
            verify( context, atLeastOnce() ).update( anyLong(), anyInt() );
            // assert that the update method on the ScheduleContext object was called and completed successfully
            // Now read the metrics back from cass and check (relies on generareJSONMetricsData from JSONMetricsContainerTest)
            final Locator locator = Locator.createLocatorFromPathComponents( "tenantOne", "mzord.duration" );
            Points<SimpleNumber> points = metricsRW.getDataToRollup(
                    locator, RollupType.BF_BASIC, new Range( start, end ),
                    CassandraModel.getBasicColumnFamilyName( Granularity.FULL ) );
            assertEquals( 1, points.getPoints().size() );

            final Locator locatorTwo = Locator.createLocatorFromPathComponents( "tenantTwo", "mzord.duration" );
            Points<SimpleNumber> pointsTwo = metricsRW.getDataToRollup(
                    locator, RollupType.BF_BASIC, new Range( start, end ),
                    CassandraModel.getBasicColumnFamilyName( Granularity.FULL ) );
            assertEquals( 1, pointsTwo.getPoints().size() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantFailureForSingleTenantHandler() throws Exception {

        HttpResponse response = httpPost( TID, postPath, generateMultitenantJSONMetricsData() );
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
            assertTrue( output[ 1 ] + " did not match no tenant id pattern " + NO_TENANT_ID_REGEX, Pattern.matches( NO_TENANT_ID_REGEX, output[ 1 ] ) );
            assertTrue( output[ 2 ] + " did not match no tenant id pattern " + NO_TENANT_ID_REGEX, Pattern.matches( NO_TENANT_ID_REGEX, output[ 2 ] ) );
            assertTrue( output[ 3 ] + " did not match no tenant id pattern " + NO_TENANT_ID_REGEX, Pattern.matches( NO_TENANT_ID_REGEX, output[ 3 ] ) );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantPartialFailureWithoutTenant() throws Exception {

        String postfix = getPostfix();
        long validTime = System.currentTimeMillis();

        String jsonBody = getJsonFromFile("sample_multi_payload_with_different_time.json", postfix);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_1%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_2%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_3%\"", validTime);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_1%", TID);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_2%", "");
        jsonBody = jsonBody.replaceAll("%TENANT_ID_3%", TID);

        HttpResponse response = httpPost( TID, postMultiPath, jsonBody );
        String[] output = getBodyArray( response );

        try {
            assertEquals("Should get status 207 from " + String.format(postMultiPath, TID), 207, response.getStatusLine().getStatusCode() );
            assertEquals("", output[0]);
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }
}
