package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionServer;

import java.io.IOException;

/**
 * HTTP Ingestion Service.
 */
public class HttpIngestionService implements IngestionService {
    private HttpMetricsIngestionServer server;
    public void startService(ScheduleContext context) {
        server = new HttpMetricsIngestionServer(context);
    }
}
