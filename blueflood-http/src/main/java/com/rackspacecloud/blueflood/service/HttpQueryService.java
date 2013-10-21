package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricDataQueryServer;

import java.io.IOException;

/**
 * HTTP Query Service.
 */
public class HttpQueryService implements QueryService {
    private HttpMetricDataQueryServer server;
    public void startService() {
        server = new HttpMetricDataQueryServer();
    }
}
