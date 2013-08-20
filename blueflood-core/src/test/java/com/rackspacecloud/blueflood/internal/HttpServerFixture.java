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

package com.rackspacecloud.blueflood.internal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpServerFixture {
    private static final String SAMPLE_ACCOUNT = "{\n" +
        "\"id\": \"ackVCKg1rk\",\n" +
        "\"external_id\": \"5821004\",\n" +
        "\"metadata\": {},\n" +
        "\"min_check_interval\": 30,\n" +
        "\"webhook_token\": \"CnXGHWj14H1koKjY8x5hFHPt5vSAAHTB\",\n" +
        "\"account_status\": \"GOOD\",\n" +
        "\"rackspace_managed\": false,\n" +
        "\"cep_group_id\": \"cgA\",\n" +
        "\"api_rate_limits\": {\n" +
        "    \"global\": 50000,\n" +
        "    \"test_check\": 500,\n" +
        "    \"test_alarm\": 500,\n" +
        "    \"test_notification\": 200,\n" +
        "    \"traceroute\": 300\n" +
        "},\n" +
        "\"limits\": {\n" +
        "    \"checks\": 10000,\n" +
        "    \"alarms\": 10000\n" +
        "},\n" +
        "\"features\": {\n" +
        "    \"agent\": true,\n" +
        "    \"rollups\": true\n" +
        "},\n" +
        "\"agent_bundle_channel\": \"stable\",\n" +
        "\"metrics_ttl\": {\n" +
        "    \"full\": 1,\n" +
        "    \"5m\": 2,\n" +
        "    \"20m\": 3,\n" +
        "    \"60m\": 4,\n" +
        //"240 is missing.    \"240m\": 5,\n" +
        "    \"1440m\": 6\n" +
        "}\n" +
        "}";
    
    private HttpServer server;
    private HttpHandler ackVCKg1rkHandler;
    private HttpHandler notFoundHandler;
    private HttpHandler interalErrorHandler;
    private final String basePath;
    private final int port;
    protected ExecutorService httpExecutor;

    public HttpServerFixture(String basePath, int port) {
        this.basePath = basePath;
        this.port = port;
    }

    public void serverUp() throws IOException {
        notFoundHandler = new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                sendResponse(404, "{ \"error\": \"404 Not Found\"}", httpExchange);
            }
        };
        ackVCKg1rkHandler = new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                sendResponse(200, SAMPLE_ACCOUNT, httpExchange);
            }
        };
        interalErrorHandler = new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                sendResponse(500, "{ \"error\": \"500 Internal error\"}", httpExchange);
            }
        };

        server = HttpServer.create(new InetSocketAddress("localhost", port), 2); // small backlog.
        server.createContext(basePath + "/accounts/ackVCKg1rk", ackVCKg1rkHandler);
        server.createContext(basePath + "/accounts/notFound", notFoundHandler);
        server.createContext(basePath + "/accounts/internalError", interalErrorHandler);
        server.createContext(basePath + "/test200", ackVCKg1rkHandler);
        server.createContext(basePath + "/test404", notFoundHandler);
        server.createContext(basePath + "/test500", interalErrorHandler);

        httpExecutor = Executors.newFixedThreadPool(1);
        server.setExecutor(httpExecutor);
        server.start();
    }
    
    protected static void sendResponse(int code, String body, HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(code, body.length());
        OutputStream out = httpExchange.getResponseBody();
        out.write(body.getBytes());
        out.close();
    }
    
    public void serverDown() {
        if (server != null)
            server.stop(0);
        ackVCKg1rkHandler = null;
        server = null;
    }

    protected String getClusterString() {
        return server.getAddress().getHostName() + ":" + server.getAddress().getPort();
    }
}
