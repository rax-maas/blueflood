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

package com.rackspacecloud.blueflood.tracker;

import com.rackspacecloud.blueflood.http.HttpRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.Metric;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tracker implements TrackerMBean {

    public static final String trackerName = String.format("com.rackspacecloud.blueflood.tracker:type=%s", Tracker.class.getSimpleName());

    private static final Logger log = LoggerFactory.getLogger(Tracker.class);

    private static final String EMPTY_STRING = "";

    private final Pattern patternGetTid = Pattern.compile( "/v\\d+\\.\\d+/([^/]+)/.*" );
    private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // Tracker is a singleton
    private static final Tracker instance = new Tracker();
    private boolean isRegistered = false;

    private Set tenantIds = new HashSet();
    private boolean isTrackingDelayedMetrics = false;
    private Set<String> metricNames = new HashSet<String>();

    // private constructor for singleton
    private Tracker(){
        // set dateFormatter to GMT since collectionTime for metrics should always be GMT/UTC
        dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public static Tracker getInstance() {
        return instance;
    }

    public synchronized void register() {
        if (isRegistered) return;

        try {
            ObjectName objectName = new ObjectName(trackerName);

            // register TrackerMBean only if not already registered
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (!mBeanServer.isRegistered(objectName)) {
                ManagementFactory.getPlatformMBeanServer().registerMBean(instance, objectName);
            }

            isRegistered = true;
            log.info("MBean registered as " + trackerName);

        } catch (Exception exc) {
            log.error("Unable to register MBean " + trackerName, exc);
        }
    }

    public void addTenant(String tenantId) {
        tenantIds.add(tenantId);
        log.info("[TRACKER] tenantId " + tenantId + " added.");
    }

    public void removeTenant(String tenantId) {
        tenantIds.remove(tenantId);
        log.info("[TRACKER] tenantId " + tenantId + " removed.");
    }

    public void removeAllTenants() {
        tenantIds.clear();
        log.info("[TRACKER] all tenants removed.");
    }

    public Set getTenants() {
        return tenantIds;
    }

    public void addMetricName(String metricName) {
        metricNames.add(metricName);
        log.info("[TRACKER] Metric name "+ metricName + " added.");
    }

    public void removeMetricName(String metricName) {
        metricNames.remove(metricName);
        log.info("[TRACKER] Metric name "+ metricName + " removed.");
    }

    public void removeAllMetricNames() {
        metricNames.clear();
        log.info("[TRACKER] All metric names removed.");
    }

    public Set<String> getMetricNames() {
        return metricNames;
    }

    public void setIsTrackingDelayedMetrics() {
        isTrackingDelayedMetrics = true;
        log.info("[TRACKER] Tracking delayed metrics started");
    }

    public void resetIsTrackingDelayedMetrics() {
        isTrackingDelayedMetrics = false;
        log.info("[TRACKER] Tracking delayed metrics stopped");
    }

    public boolean getIsTrackingDelayedMetrics() {
        return isTrackingDelayedMetrics;
    }

    public boolean isTracking(String tenantId) {
        return tenantIds.contains(tenantId);
    }

    public boolean doesMessageContainMetricNames(String logmessage) {
        boolean toLog = false;

        if (metricNames.size() == 0) {
            toLog = true;
        }
        else {
            for (String name : metricNames) {
                if (logmessage.contains(name)) {
                    toLog = true;
                    break;
                }
            }
        }
        return toLog;
    }

    public void track(HttpRequest request) {
        // check if tenantId is being tracked by JMX TenantTrackerMBean and log the request if it is
        if (request == null) return;

        String tenantId = findTid( request.getUri() );

        if (isTracking(tenantId)) {

            // get headers
            String headers = EMPTY_STRING;
            for (String headerName : request.headers().names()) {
                headers += "\n" + headerName + "\t" + request.headers().get(headerName);
            }

            // get parameters
            String queryParams = getQueryParameters(request);

            // get request content
            String requestContent = EMPTY_STRING;
            if ( request instanceof FullHttpRequest ) {
                FullHttpRequest fullReq = (FullHttpRequest)request;
                requestContent = fullReq.content().toString(Constants.DEFAULT_CHARSET);
                if ((requestContent != null) && (!requestContent.isEmpty())) {
                    requestContent = "\nREQUEST_CONTENT:\n" + requestContent;
                }
            }

            // log request
            String logMessage = "[TRACKER] " +
                    request.getMethod() + " request for tenantId " + tenantId + ": " + request.getUri() + queryParams + "\n" +
                    "HEADERS: " + headers +
                    requestContent;

            if (doesMessageContainMetricNames(logMessage)) {
                log.info(logMessage);
            }
        }
    }

    public void trackResponse(HttpRequest request, FullHttpResponse response) {
        // check if tenantId is being tracked by JMX TenantTrackerMBean and log the response if it is
        // HttpRequest is needed for original request uri and tenantId
        if (request == null) return;
        if (response == null) return;

        String tenantId = findTid( request.getUri() );
        if (isTracking(tenantId)) {
            HttpResponseStatus status = response.getStatus();
            String messageBody = response.content().toString(Constants.DEFAULT_CHARSET);

            // get parameters
            String queryParams = getQueryParameters(request);

            // get headers
            String headers = "";
            for (String headerName : response.headers().names()) {
                headers += "\n" + headerName + "\t" + response.headers().get(headerName);
            }

            // get response content
            String responseContent = "";
            if ((messageBody != null) && (!messageBody.isEmpty())) {
                responseContent = "\nRESPONSE_CONTENT:\n" + messageBody;
            }

            String logMessage = "[TRACKER] " +
                    "Response for tenantId " + tenantId + " " + request.getMethod() + " request " + request.getUri() + queryParams +
                    "\nRESPONSE_STATUS: " + status.code() +
                    "\nRESPONSE HEADERS: " + headers +
                    responseContent;

            log.info(logMessage);
        }

    }

    /**
     * This method is used to log delayed metrics, if tracking delayed metrics
     * is turned on for this Blueflood service.
     * @param tenantid
     * @param delayedMetrics
     */
    public void trackDelayedMetricsTenant(String tenantid, final List<Metric> delayedMetrics) {
        if (isTrackingDelayedMetrics) {
            String logMessage = String.format("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics %s", tenantid);
            log.info(logMessage);

            // log individual delayed metrics locator and collectionTime
            double delayedMinutes;
            long nowMillis = System.currentTimeMillis();
            for (Metric metric : delayedMetrics) {
                delayedMinutes = (double)(nowMillis - metric.getCollectionTime()) / 1000 / 60;
                logMessage = String.format("[TRACKER][DELAYED METRIC] %s has collectionTime %s which is delayed by %.2f minutes",
                        metric.getLocator().toString(),
                        dateFormatter.format(new Date(metric.getCollectionTime())),
                        delayedMinutes);
                log.info(logMessage);
            }
        }
    }

    /**
     * This method logs the delayed aggregated metrics for a particular tenant,
     * if tracking delayed metric is turned on for this Blueflood service.
     * Aggregated metrics have one single timestamp for the group of metrics that
     * are sent in one request.
     *
     * @param tenantId              the tenantId who's the sender of the metrics
     * @param collectionTimeMs      the collection timestamp (ms) in request payload
     * @param delayTimeMs           the delayed time (ms)
     * @param delayedMetricNames    the list of delayed metrics in request payload
     */
    public void trackDelayedAggregatedMetricsTenant(String tenantId, long collectionTimeMs, long delayTimeMs, List<String> delayedMetricNames) {
        if (isTrackingDelayedMetrics) {
            String logMessage = String.format("[TRACKER][DELAYED METRIC] Tenant sending delayed metrics %s", tenantId);
            log.info(logMessage);

            // log individual delayed metrics locator and collectionTime
            double delayMin = delayTimeMs / 1000 / 60;
            logMessage = String.format("[TRACKER][DELAYED METRIC] %s have collectionTime %s which is delayed by %.2f minutes",
                    StringUtils.join(delayedMetricNames, ","),
                        dateFormatter.format(new Date(collectionTimeMs)),
                        delayMin);
            log.info(logMessage);
        }
    }

    String getQueryParameters(HttpRequest httpRequest) {
        String params = "";
        Map<String, List<String>> parameters = ((HttpRequestWithDecodedQueryParams) httpRequest).getQueryParams();

        for (Map.Entry<String, List<String>> param : parameters.entrySet()) {
            String paramName = param.getKey();
            List<String> paramValues = param.getValue();

            for (String paramValue : paramValues) {
                if (!params.equals("")) {
                    params += "&";
                }
                params += paramName + "=" + paramValue;
            }
        }

        if (!params.equals("")) params = "?" + params;
        return params;
    }

    String findTid( String uri ) {

        Matcher m = patternGetTid.matcher( uri );

        if( m.matches() )
            return m.group( 1 );
        else
            return null;
    }
}
