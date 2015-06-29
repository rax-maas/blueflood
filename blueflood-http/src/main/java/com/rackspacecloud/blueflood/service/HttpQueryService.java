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
import com.rackspacecloud.blueflood.outputs.handlers.HttpMetricDataQueryServer;

import java.io.IOException;

/**
 * HTTP Query Service.
 */
public class HttpQueryService implements QueryService {
    private HttpMetricDataQueryServer server;
    public void startService() {
        getServer().startServer();
    }

    @VisibleForTesting
    public void stopService() { server.stopServer();}

    @VisibleForTesting
    public void setServer(HttpMetricDataQueryServer srv) {
        server = srv;
    }

    private HttpMetricDataQueryServer getServer() {
        if (server == null) {
            server = new HttpMetricDataQueryServer();
        }

        return server;
    }
}
