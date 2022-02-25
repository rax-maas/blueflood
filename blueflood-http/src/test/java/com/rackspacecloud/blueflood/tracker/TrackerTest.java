package com.rackspacecloud.blueflood.tracker;

import com.rackspacecloud.blueflood.http.HttpRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import junit.framework.Assert;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
@PowerMockIgnore( {"javax.management.*"})
public class TrackerTest {
    Tracker tracker = Tracker.getInstance();
    String testTenant1 = "tenant1";
    String testTenant2 = "tenant2";
    String tenantId = "121212";

    private static Logger loggerMock;
    private HttpRequestWithDecodedQueryParams httpRequestMock;
    private HttpMethod httpMethodMock;
    private FullHttpResponse httpResponseMock;
    private HttpResponseStatus httpResponseStatusMock;
    private Map<String, List<String>> queryParams;
    private List<Metric> delayedMetrics;
    private long collectionTime;

    @Captor
    ArgumentCaptor argCaptor;

    @BeforeClass
    public static void setupClass() {
        // mock logger
        PowerMockito.mockStatic(LoggerFactory.class);
        loggerMock = mock(Logger.class);
        when(LoggerFactory.getLogger(any(Class.class))).thenReturn(loggerMock);
    }

    @Before
    public void setUp() {
        // reset static logger mock for every new test setup
        reset(loggerMock);

        // mock HttpRequest, method, queryParams, channelBuffer, responseStatus
        httpRequestMock = mock(HttpRequestWithDecodedQueryParams.class);
        httpMethodMock = mock(HttpMethod.class);
        queryParams = new HashMap<String, List<String>>();

        // mock HttpResponse and HttpResponseStatus
        httpResponseMock = mock(FullHttpResponse.class);
        httpResponseStatusMock = mock(HttpResponseStatus.class);

        // mock headers
        HttpHeaders headers = mock(HttpHeaders.class);
        Set<String> headerNames = new HashSet<String>();
        headerNames.add("X-Auth-Token");
        when(httpRequestMock.headers()).thenReturn(headers);
        when(headers.names()).thenReturn(headerNames);
        when(headers.get("X-Auth-Token")).thenReturn("AUTHTOKEN");

        HttpHeaders responseHeaders = mock(HttpHeaders.class);
        when(httpResponseMock.headers()).thenReturn(responseHeaders);
        when(responseHeaders.names()).thenReturn(new HashSet<String>());

        // setup delayed metrics
        Locator locator1 = Locator.createLocatorFromPathComponents(tenantId, "delayed", "metric1");
        Locator locator2 = Locator.createLocatorFromPathComponents(tenantId, "delayed", "metric2");
        collectionTime = 1451606400000L;      // Jan 1, 2016 UTC
        TimeValue ttl = new TimeValue(3, TimeUnit.DAYS);
        Metric delayMetric1 = new Metric(locator1, 123, collectionTime, ttl, "");
        Metric delayMetric2 = new Metric(locator2, 456, collectionTime, ttl, "");
        delayedMetrics = new ArrayList<Metric>();
        delayedMetrics.add(delayMetric1);
        delayedMetrics.add(delayMetric2);
    }

    @Test
    public void testRegister() {
        // register the tracker and verify
        tracker.register();
        verify(loggerMock, times(1)).info("MBean registered as com.rackspacecloud.blueflood.tracker:type=Tracker");
    }

    @Test
    public void testAddTenant() {
        tracker.addTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenants.size not 1", tenants.size() == 1 );
        assertTrue( "tenants does not contain " + testTenant1, tenants.contains( testTenant1 ) );
    }

    @Test
    public void testDoesNotAddTenantTwice() {
        tracker.addTenant(testTenant1);
        tracker.addTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenants.size not 1", tenants.size() == 1 );
    }

    @Test
    public void testRemoveTenant() {
        tracker.addTenant(testTenant1);
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );

        tracker.removeTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertFalse( "tenant " + testTenant1 + " not removed", tracker.isTracking( testTenant1 ) );
        assertEquals( "tenants.size not 0", tenants.size(), 0 );
        assertFalse( "tenants contains " + testTenant1, tenants.contains( testTenant1 ) );
    }

    @Test
    public void testAddAndRemoveMetricName() {
        String metric1 = "metricName";
        String metric2 = "anotherMetricName";

        tracker.addMetricName(metric1);
        assertTrue(metric1 + " not added", tracker.doesMessageContainMetricNames("Track.this." + metric1));

        tracker.addMetricName(metric2);
        assertTrue(metric1 + " not being logged", tracker.doesMessageContainMetricNames("Track.this." + metric1));
        assertTrue(metric2 + "not being logged", tracker.doesMessageContainMetricNames("Track.this." + metric2));
        assertFalse("randomMetricNameNom should not be logged", tracker.doesMessageContainMetricNames("Track.this.randomMetricNameNom"));

        Set<String> metricNames = tracker.getMetricNames();
        assertTrue("metricNames should contain " + metric1, metricNames.contains(metric1));
        assertTrue("metricNames should contain " + metric2, metricNames.contains(metric2));

        tracker.removeMetricName(metric1);
        assertFalse(metric1 + " should not be logged", tracker.doesMessageContainMetricNames("Track.this." + metric1));

        metricNames = tracker.getMetricNames();
        assertFalse("metricNames should not contain " + metric1, metricNames.contains(metric1));
        assertTrue("metricNames should contain " + metric2, metricNames.contains(metric2));
    }

    @Test
    public void testRemoveAllMetricNames() {
        tracker.addMetricName("metricName");
        tracker.addMetricName("anotherMetricNom");

        assertTrue("metricName not being logged",tracker.doesMessageContainMetricNames("Track.this.metricName"));
        assertTrue("anotherMetricNom not being logged",tracker.doesMessageContainMetricNames("Track.this.anotherMetricNom"));
        assertFalse("randomMetricNameNom should not be logged", tracker.doesMessageContainMetricNames("Track.this.randomMetricNameNom"));

        tracker.removeAllMetricNames();

        assertTrue("Everything should be logged", tracker.doesMessageContainMetricNames("Track.this.metricName"));
        assertTrue("Everything should be logged", tracker.doesMessageContainMetricNames("Track.this.anotherMetricNom"));
        assertTrue("Everything should be logged", tracker.doesMessageContainMetricNames("Track.this.randomMetricNameNom"));
    }

    @Test
    public void testRemoveAllTenant() {
        tracker.addTenant(testTenant1);
        tracker.addTenant(testTenant2);
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenant " + testTenant2 + " not added", tracker.isTracking( testTenant2 ) );

        tracker.removeAllTenants();
        assertFalse( "tenant " + testTenant1 + " not removed", tracker.isTracking( testTenant1 ) );
        assertFalse( "tenant " + testTenant2 + " not removed", tracker.isTracking( testTenant2 ) );

        Set tenants = tracker.getTenants();
        assertEquals( "tenants.size not 0", tenants.size(), 0 );
    }

    @Test
    public void testFindTidFound() {
        assertEquals( tracker.findTid( "/v2.0/6000/views" ), "6000" );
    }

    @Test
     public void testTrackTenantNoVersion() {
        assertEquals( tracker.findTid( "/6000/views" ), null );
    }

    @Test
    public void testTrackTenantBadVersion() {
        assertEquals( tracker.findTid( "blah/6000/views" ), null );
    }

    @Test
    public void testTrackTenantTrailingSlash() {
        assertEquals( tracker.findTid( "/v2.0/6000/views/" ), "6000" );
    }

    @Test
    public void testSetIsTrackingDelayedMetrics() {
        tracker.resetIsTrackingDelayedMetrics();
        tracker.setIsTrackingDelayedMetrics();
        Assert.assertTrue("isTrackingDelayedMetrics should be true from setIsTrackingDelayedMetrics", tracker.getIsTrackingDelayedMetrics());
    }

    @Test
    public void testResetIsTrackingDelayedMetricss() {
        tracker.setIsTrackingDelayedMetrics();
        tracker.resetIsTrackingDelayedMetrics();
        Assert.assertFalse("isTrackingDelayedMetrics should be false from resetIsTrackingDelayedMetrics", tracker.getIsTrackingDelayedMetrics());
    }

    @Test
    public void testTrackTenant() throws Exception {

        // setup mock returns for ingest POST
        when(httpRequestMock.getUri()).thenReturn("/v2.0/" + tenantId + "/ingest");
        when(httpMethodMock.toString()).thenReturn("POST");
        when(httpRequestMock.getMethod()).thenReturn(httpMethodMock);

        List<String> paramValues = new ArrayList<String>();
        paramValues.add("value1");
        paramValues.add("value2");
        queryParams.put("param1", paramValues);
        when((httpRequestMock).getQueryParams()).thenReturn(queryParams);

        String payload =        "[\n" +
                "      {\n" +
                "        \"collectionTime\": 1376509892612,\n" +
                "        \"ttlInSeconds\": 172800,\n" +
                "        \"metricValue\": 65,\n" +
                "        \"metricName\": \"example.metric.one\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"collectionTime\": 1376509892612,\n" +
                "        \"ttlInSeconds\": 172800,\n" +
                "        \"metricValue\": 66,\n" +
                "        \"metricName\": \"example.metric.two\"\n" +
                "      },\n" +
                "    ]'";
        when(httpRequestMock.content()).thenReturn(Unpooled.copiedBuffer(payload.getBytes("UTF-8")));

        // add tenant and track
        tracker.addTenant(tenantId);
        tracker.track(httpRequestMock);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] tenantId " + tenantId + " added.");
        verify(loggerMock, atLeastOnce()).info((String)argCaptor.capture());
        assertThat(argCaptor.getValue().toString(), containsString("[TRACKER] POST request for tenantId " + tenantId + ": /v2.0/" + tenantId + "/ingest?param1=value1&param1=value2\n" +
                "HEADERS: \n" +
                "X-Auth-Token\tAUTHTOKEN\n" +
                "REQUEST_CONTENT:\n" +
                "[\n" +
                "      {\n" +
                "        \"collectionTime\": 1376509892612,\n" +
                "        \"ttlInSeconds\": 172800,\n" +
                "        \"metricValue\": 65,\n" +
                "        \"metricName\": \"example.metric.one\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"collectionTime\": 1376509892612,\n" +
                "        \"ttlInSeconds\": 172800,\n" +
                "        \"metricValue\": 66,\n" +
                "        \"metricName\": \"example.metric.two\"\n" +
                "      },\n" +
                "    ]'"));
    }

    @Test
    public void testTrackNullRequest() {
        when(httpRequestMock.getUri()).thenReturn("/v2.0/" + tenantId + "/ingest");

        // add tenant to track but provide a null request for tracking
        tracker.addTenant(tenantId);
        tracker.track(null);

        // verify logger does not get called for tracking request
        verify(httpRequestMock, never()).getUri();
        verify(loggerMock, atLeastOnce()).info((String)argCaptor.capture());
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] POST request for tenantId " + tenantId)));
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] GET request for tenantId " + tenantId)));
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] PUT request for tenantId " + tenantId)));
    }

    @Test
    public void testDoesNotTrackTenant() {
        // setup mock returns URI
        when(httpRequestMock.getUri()).thenReturn("/v2.0/" + tenantId + "/ingest");

        // make sure tenantId is removed and track
        tracker.removeTenant(tenantId);
        tracker.track(httpRequestMock);

        // verify does not log for tenantId
        verify(loggerMock, atLeastOnce()).info((String)argCaptor.capture());
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] POST request for tenantId " + tenantId)));
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] GET request for tenantId " + tenantId)));
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] PUT request for tenantId " + tenantId)));
    }

    @Test
    public void testTrackResponse() throws Exception {

        // setup mock returns for query POST
        String requestUri = "/v2.0/" + tenantId + "/metrics/search";
        when(httpRequestMock.getUri()).thenReturn(requestUri);
        when(httpMethodMock.toString()).thenReturn("GET");
        when(httpRequestMock.getMethod()).thenReturn(httpMethodMock);

        List<String> paramValues = new ArrayList<String>();
        paramValues.add("locator1");
        queryParams.put("query", paramValues);
        when((httpRequestMock).getQueryParams()).thenReturn(queryParams);

        when(httpResponseStatusMock.code()).thenReturn(200);
        when(httpResponseMock.getStatus()).thenReturn(httpResponseStatusMock);

        //when(channelBufferMock.toString(any(Charset.class))).thenReturn(
        String result = "[TRACKER] Response for tenantId " + tenantId;
        when(httpResponseMock.content()).thenReturn(Unpooled.copiedBuffer(result.getBytes("UTF-8")));

        // add tenant and track
        tracker.addTenant(tenantId);
        tracker.trackResponse(httpRequestMock, httpResponseMock);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] tenantId " + tenantId + " added.");
        verify(loggerMock, times(1)).info("[TRACKER] Response for tenantId " + tenantId + " GET request " + requestUri + "?query=locator1\n" +
                "RESPONSE_STATUS: 200\n" +
                "RESPONSE HEADERS: \n" +
                "RESPONSE_CONTENT:\n" +
                "[TRACKER] Response for tenantId " + tenantId);
    }

    @Test
    public void testTrackNullResponse() {
        String requestUri = "/v2.0/" + tenantId + "/metrics/search";
        when(httpRequestMock.getUri()).thenReturn(requestUri);

        // add tenant to track but provide a null response for tracking
        tracker.addTenant(tenantId);
        tracker.trackResponse(httpRequestMock, null);

        // verify logger does not get called for tracking response
        verify(httpRequestMock, never()).getUri();
        verify(loggerMock, atLeastOnce()).info((String)argCaptor.capture());
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] Response for tenantId " + tenantId)));
    }

    @Test
    public void testDoesNotTrackTenantResponse() {
        // setup mock returns URI
        when(httpRequestMock.getUri()).thenReturn("/v2.0/" + tenantId + "/metrics/search");

        // make sure tenantId is removed and track
        tracker.removeTenant(tenantId);
        tracker.trackResponse(httpRequestMock, httpResponseMock);

        // verify does not log for tenantId
        verify(loggerMock, atLeastOnce()).info((String)argCaptor.capture());
        assertThat(argCaptor.getValue().toString(), not(containsString("[TRACKER] Response for tenantId  " + tenantId)));
    }

    @Test
    public void testTrackDelayedMetricsTenant() {
        // enable tracking delayed metrics and track
        tracker.setIsTrackingDelayedMetrics();
        tracker.trackDelayedMetricsTenant(tenantId, delayedMetrics);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] Tracking delayed metrics started");
        verify(loggerMock, atLeastOnce()).info("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics " + tenantId);
        verify(loggerMock, atLeastOnce()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric1 has collectionTime 2016-01-01 00:00:00 which is delayed"));
        verify(loggerMock, atLeastOnce()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric2 has collectionTime 2016-01-01 00:00:00 which is delayed"));
    }

    @Test
    public void testDoesNotTrackDelayedMetricsTenant() {
        // disable tracking delayed metrics and track
        tracker.resetIsTrackingDelayedMetrics();
        tracker.trackDelayedMetricsTenant(tenantId, delayedMetrics);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] Tracking delayed metrics stopped");
        verify(loggerMock, never()).info("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics " + tenantId);
        verify(loggerMock, never()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric1 has collectionTime 2016-01-01 00:00:00"));
        verify(loggerMock, never()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric2 has collectionTime 2016-01-01 00:00:00"));
    }

    @Test
    public void testTrackDelayedAggregatedMetricsTenant() {
        // enable tracking delayed metrics and track
        tracker.setIsTrackingDelayedMetrics();

        List<String> delayedMetricNames = new ArrayList<String>() {{
            for ( Metric metric : delayedMetrics ) {
                add(metric.getLocator().toString());
            }
        }};
        long ingestTime = System.currentTimeMillis();
        tracker.trackDelayedAggregatedMetricsTenant(tenantId, delayedMetrics.get(0).getCollectionTime(), ingestTime, delayedMetricNames);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] Tracking delayed metrics started");
        verify(loggerMock, atLeastOnce()).info("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics " + tenantId);
        verify(loggerMock, atLeastOnce()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric1" + "," +
                                                            tenantId + ".delayed.metric2 have collectionTime 2016-01-01 00:00:00 which is delayed"));
    }

    @Test
    public void testDoesNotTrackDelayedAggregatedMetricsTenant() {
        // disable tracking delayed metrics and track
        tracker.resetIsTrackingDelayedMetrics();
        tracker.trackDelayedMetricsTenant(tenantId, delayedMetrics);

        // verify
        verify(loggerMock, atLeastOnce()).info("[TRACKER] Tracking delayed metrics stopped");
        verify(loggerMock, never()).info("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics " + tenantId);
        verify(loggerMock, never()).info(contains("[TRACKER][DELAYED METRIC] " + tenantId + ".delayed.metric1" + "," +
                tenantId + ".delayed.metric2 have collectionTime 2016-01-01 00:00:00 which is delayed"));
    }
}
