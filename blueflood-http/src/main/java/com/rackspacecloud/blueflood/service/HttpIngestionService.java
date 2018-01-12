/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.service;

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionServer;

/**
 * HTTP Ingestion Service.
 */
public class HttpIngestionService implements IngestionService {
    private HttpMetricsIngestionServer server;
    private ScheduleContext context;

    public void startService(ScheduleContext context) throws InterruptedException {
        this.context = context;

        getHttpMetricsIngestionServer().startServer();
    }

    @VisibleForTesting
    public void setMetricsIngestionServer(HttpMetricsIngestionServer server) {
        this.server = server;
    }

    protected HttpMetricsIngestionServer getHttpMetricsIngestionServer() {
        if (this.server == null) {
            this.server = new HttpMetricsIngestionServer(this.context);
        }

        return this.server;
    }

    public void shutdownService() {
        if (this.server != null) {
            this.server.shutdownServer();
        }
    }
}
