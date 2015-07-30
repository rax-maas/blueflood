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

import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.io.Constants;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Tracker implements TrackerMBean {

    private static final Logger log = LoggerFactory.getLogger(Tracker.class);
    private static final String trackerName = String.format("com.rackspacecloud.blueflood.tracker:type=%s", Tracker.class.getSimpleName());

    static Set tenantIds = new HashSet();

    public Tracker() {
        registerMBean();
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

    public static boolean isTracking(String tenantId) {
        return tenantIds.contains(tenantId);
    }

    public Set getTenants() {
        return tenantIds;
    }

    public void registerMBean() {
        try {
            ObjectName objectName = new ObjectName(trackerName);

            // register TrackerMBean only if not already registered
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (!mBeanServer.isRegistered(objectName)) {
                ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
            }
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + this.getClass().getSimpleName(), exc);
        }
    }

    public static void track(HttpRequest request) {
        // check if tenantId is being tracked by JMX TenantTrackerMBean and log the request if it is
        if (request == null) return;

        String tenantId = request.getHeader("tenantId");
        if (isTracking(tenantId)) {

            // get headers
            String headers = "";
            for (String headerName : request.getHeaderNames()) {
                headers += "\n" + headerName + "\t" + request.getHeader(headerName);
            }

            // get parameters
            String queryParams = getQueryParameters(request);

            // get request content
            String requestContent = request.getContent().toString(Constants.DEFAULT_CHARSET);
            if ((requestContent != null) && (!requestContent.isEmpty())) {
                requestContent = "\nREQUEST_CONTENT:\n" + requestContent;
            }

            // log request
            String logMessage = "[TRACKER] " +
                    request.getMethod().toString() + " request for tenantId " + tenantId + ": " + request.getUri() + queryParams + "\n" +
                    "HEADERS: " + headers +
                    requestContent;

            log.info(logMessage);
        }
    }

    public static void trackResponse(HttpRequest request, HttpResponse response) {
        // check if tenantId is being tracked by JMX TenantTrackerMBean and log the response if it is
        // HttpRequest is needed for original request uri and tenantId
        if (request == null) return;
        if (response == null) return;

        String tenantId = request.getHeader("tenantId");
        if (isTracking(tenantId)) {
            HttpResponseStatus status = response.getStatus();
            String messageBody = response.getContent().toString(Constants.DEFAULT_CHARSET);

            // get parameters
            String queryParams = getQueryParameters(request);

            // get response content
            String responseContent = "";
            if ((messageBody != null) && (!messageBody.isEmpty())) {
                responseContent = "\nRESPONSE_CONTENT:\n" + messageBody;
            }

            String logMessage = "[TRACKER] " +
                    "Response for tenantId " + tenantId + " request " + request.getUri() + queryParams + "\n" +
                    "RESPONSE_STATUS: " + status.getCode() +
                    responseContent;

            log.info(logMessage);
        }

    }

    static String getQueryParameters(HttpRequest httpRequest) {
        String params = "";
        Map<String, List<String>> parameters = ((HTTPRequestWithDecodedQueryParams) httpRequest).getQueryParams();

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

}
