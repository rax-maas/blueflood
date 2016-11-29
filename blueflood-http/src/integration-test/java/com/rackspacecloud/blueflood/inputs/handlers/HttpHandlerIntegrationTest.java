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

import com.google.common.net.HttpHeaders;
import com.rackspacecloud.blueflood.http.HttpIntegrationTestBase;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.ErrorResponse;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static com.rackspacecloud.blueflood.TestUtils.*;

public class HttpHandlerIntegrationTest extends HttpIntegrationTestBase {

    static private final String TENANT_ID = "acTEST";
    static private long BEFORE_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS );
    static private long AFTER_CURRENT_COLLECTIONTIME_MS = Configuration.getInstance().getLongProperty( CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS );

    //A time stamp 2 days ago
    private final long baseMillis = Calendar.getInstance().getTimeInMillis() - 172800000;

    @Test
    public void testHttpIngestionHappyCase() throws Exception {

        String postfix = getPostfix();

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        HttpResponse response = postGenMetric(TENANT_ID, postfix, postPath );
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

        long time = System.currentTimeMillis() - TIME_DIFF_MS - BEFORE_CURRENT_COLLECTIONTIME_MS;

        HttpResponse response = postGenMetric(TENANT_ID, postfix, postPath, time );

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals( 400, response.getStatusLine().getStatusCode() );
        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        for (int i = 0; i < 3; i++) {
            assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(i).getSource());
            assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                    " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(i).getMessage());
        }

    }

    @Test
    public void testHttpIngestionInvalidFutureCollectionTime() throws Exception {

        String postfix = getPostfix();

        long time = System.currentTimeMillis() + TIME_DIFF_MS + AFTER_CURRENT_COLLECTIONTIME_MS;

        HttpResponse response = postGenMetric(TENANT_ID, postfix, postPath, time );

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());
        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        for (int i = 0; i < 3; i++) {
            assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(i).getSource());
            assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                    " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(i).getMessage());
        }
    }

    @Test
    public void testHttpIngestionPartialInvalidCollectionTime() throws Exception {
        String postfix = getPostfix();
        long validTime = System.currentTimeMillis();
        long pastTime = System.currentTimeMillis() - TIME_DIFF_MS - BEFORE_CURRENT_COLLECTIONTIME_MS;
        long futureTime = System.currentTimeMillis() + TIME_DIFF_MS + AFTER_CURRENT_COLLECTIONTIME_MS;

        String jsonBody = getJsonFromFile("sample_multi_payload_with_different_time.json", postfix);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_1%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_2%\"", pastTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_3%\"", futureTime);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_.%", TENANT_ID);

        HttpResponse response = httpPost( TENANT_ID, postMultiPath, jsonBody );
        ErrorResponse errorResponse = getErrorResponse(response);

        try {
            assertEquals("Should get status 207 from " + String.format(postMultiPath, TENANT_ID), 207, response.getStatusLine().getStatusCode() );
            for (int i = 0; i < 4; i++) {
                assertEquals("Invalid error source", "collectionTime", errorResponse.getErrors().get(i).getSource());
                assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                        " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(i).getMessage());
            }
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testHttpAggregatedIngestionInvalidPastCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - BEFORE_CURRENT_COLLECTIONTIME_MS;

        HttpResponse response = postMetric("333333", postAggregatedPath, "sample_payload.json", timestamp, getPostfix());
        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testHttpAggregatedIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + AFTER_CURRENT_COLLECTIONTIME_MS;

        HttpResponse response = postMetric("333333", postAggregatedPath, "sample_payload.json", timestamp, getPostfix());
        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());
        assertEquals("Number of errors invalid", 1, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
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

        long timestamp = System.currentTimeMillis() - TIME_DIFF_MS - BEFORE_CURRENT_COLLECTIONTIME_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric("333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json",
                timestamp,
                postfix);

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());

        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }


    @Test
    public void testHttpAggregatedMultiIngestionInvalidFutureCollectionTime() throws IOException, URISyntaxException {

        long timestamp = System.currentTimeMillis() + TIME_DIFF_MS + AFTER_CURRENT_COLLECTIONTIME_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric( "333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json",
                timestamp,
                postfix );

        ErrorResponse errorResponse = getErrorResponse(response);

        assertEquals(400, response.getStatusLine().getStatusCode());

        assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
        assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(0).getSource());
        assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(0).getMessage());
    }

    @Test
    public void testHttpAggregatedMultiIngestionHappyCase() throws Exception {

        long start = System.currentTimeMillis() - TIME_DIFF_MS;
        long end = System.currentTimeMillis() + TIME_DIFF_MS;

        String postfix = getPostfix();

        HttpResponse response = postMetric("333333", postAggregatedMultiPath, "sample_multi_aggregated_payload.json", postfix);

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
        long pastTime = System.currentTimeMillis() - TIME_DIFF_MS - BEFORE_CURRENT_COLLECTIONTIME_MS;
        long futureTime = System.currentTimeMillis() + TIME_DIFF_MS + AFTER_CURRENT_COLLECTIONTIME_MS;

        String jsonBody = getJsonFromFile("sample_multi_aggregated_payload_with_different_time.json", postfix);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_1%\"", validTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_2%\"", pastTime);
        jsonBody = updateTimeStampJson(jsonBody, "\"%TIMESTAMP_3%\"", futureTime);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_.%", TENANT_ID);

        HttpResponse response = httpPost(TENANT_ID, postAggregatedMultiPath, jsonBody );
        ErrorResponse errorResponse = getErrorResponse(response);

        try {
            assertEquals("Should get status 207 from " + String.format(postAggregatedMultiPath, TENANT_ID), 207, response.getStatusLine().getStatusCode() );
            for (int i = 0; i < 2; i++) {
                assertEquals("Invalid error source", "timestamp", errorResponse.getErrors().get(i).getSource());
                assertEquals("Invalid error message", "Out of bounds. Cannot be more than " + BEFORE_CURRENT_COLLECTIONTIME_MS + " milliseconds into the past." +
                        " Cannot be more than " + AFTER_CURRENT_COLLECTIONTIME_MS + " milliseconds into the future", errorResponse.getErrors().get(i).getMessage());
            }
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

        HttpResponse response = postMetric("333333", postAggregatedMultiPath, "sample_multi_enums_payload.json", postfix);

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


        HttpResponse response = httpPost(TENANT_ID, postPath, "" );

        try {
            assertEquals( 400, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }

        response = httpPost(TENANT_ID, postPath, "Some incompatible json body" );

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
                .setPath("/v2.0/acTEST/ingest");

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

        HttpResponse response = httpPost(TENANT_ID, postMultiPath, generateMultitenantJSONMetricsData() );

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

        HttpResponse response = httpPost(TENANT_ID, postPath, generateMultitenantJSONMetricsData() );
        try {
            assertEquals( 400, response.getStatusLine().getStatusCode() );
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    @Test
    public void testMultiTenantFailureWithoutTenant() throws Exception {

        HttpResponse response = postGenMetric(TENANT_ID, "", postMultiPath );
        ErrorResponse errorResponse = getErrorResponse(response);

        try {
            assertEquals(400, response.getStatusLine().getStatusCode());
            assertEquals("Number of errors invalid", 3, errorResponse.getErrors().size());
            for (int i = 0; i < 3; i++) {
                assertEquals("Invalid error source", "tenantId", errorResponse.getErrors().get(i).getSource());
                assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(0).getMessage());
            }
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
        jsonBody = jsonBody.replaceAll("%TENANT_ID_1%", TENANT_ID);
        jsonBody = jsonBody.replaceAll("%TENANT_ID_2%", "");
        jsonBody = jsonBody.replaceAll("%TENANT_ID_3%", TENANT_ID);

        HttpResponse response = httpPost(TENANT_ID, postMultiPath, jsonBody);
        ErrorResponse errorResponse = getErrorResponse(response);

        try {
            assertEquals("Should get status 207 from " + String.format(postMultiPath, TENANT_ID), 207, response.getStatusLine().getStatusCode());
            for (int i = 0; i < 2; i++) {
                assertEquals("Invalid error source", "tenantId", errorResponse.getErrors().get(i).getSource());
                assertEquals("Invalid error message", "may not be empty", errorResponse.getErrors().get(i).getMessage());
            }
        }
        finally {
            EntityUtils.consume( response.getEntity() ); // Releases connection apparently
        }
    }

    private ErrorResponse getErrorResponse(HttpResponse response) throws IOException {
        return new ObjectMapper().readValue(response.getEntity().getContent(), ErrorResponse.class);
    }

    @Test
    public void testIngestWithContentTypeNonJsonShouldReturn415() throws Exception {

        HttpResponse response = httpPost(TENANT_ID, postPath, "random text", ContentType.APPLICATION_SVG_XML);
        assertEquals("content-type non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testIngestMultiWithContentTypeNonJsonShouldReturn415() throws Exception {

        HttpResponse response = httpPost(TENANT_ID, postMultiPath, "random text", ContentType.APPLICATION_SVG_XML );
        assertEquals("content-type non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testIngestAggregatedWithContentTypeNonJsonShouldReturn415() throws Exception {

        HttpResponse response = httpPost(TENANT_ID, postAggregatedPath, "random text", ContentType.APPLICATION_SVG_XML );
        assertEquals("content-type non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testIngestAggregatedMultiWithContentTypeNonJsonShouldReturn415() throws Exception {

        HttpResponse response = httpPost(TENANT_ID, postAggregatedMultiPath, "random text", ContentType.APPLICATION_SVG_XML );
        assertEquals("content-type non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testIngestEventWithContentTypeNonJsonShouldReturn415() throws Exception {

        HttpResponse response = httpPost(TENANT_ID, postEventsPath, "random text", ContentType.APPLICATION_SVG_XML );
        assertEquals("content-type non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testSearchMetricsWithAcceptNonJsonShouldReturn415() throws Exception {

        // test the GET /v2.0/%s/metrics/search
        URIBuilder query_builder = getMetricDataQueryURIBuilder()
                .setPath(String.format(getSearchPath, TENANT_ID));
        query_builder.setParameter("query", "foo.bar.none");
        HttpGet searchRequest = new HttpGet(query_builder.build());
        searchRequest.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_ATOM_XML.toString());
        HttpResponse response = client.execute(searchRequest);
        assertEquals("accept non Json should get 415", 415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetViewWithAcceptNonJsonShouldReturn415() throws Exception {
        // test the GET /v2.0/%s/views/%s
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getQueryMetricViewsURI(TENANT_ID, "bogus.name"));
        get.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_ATOM_XML.toString());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetAnnotationWithAcceptNonJsonShouldReturn415() throws Exception {
        // test the GET /v2.0/%s/events
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getQueryEventsURI(TENANT_ID));
        get.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_ATOM_XML.toString());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(415, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testGetTokenSearchWithAcceptNonJsonShouldReturn415() throws Exception {
        // test the GET /v2.0/%s/metrics_name/search
        parameterMap = new HashMap<String, String>();
        parameterMap.put(Event.fromParameterName, String.valueOf(baseMillis - 86400000));
        parameterMap.put(Event.untilParameterName, String.valueOf(baseMillis + (86400000*3)));
        HttpGet get = new HttpGet(getQueryTokenSearchURI(TENANT_ID));
        get.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_ATOM_XML.toString());
        HttpResponse response = client.execute(get);
        Assert.assertEquals(415, response.getStatusLine().getStatusCode());
    }
}
