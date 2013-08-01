package com.cloudkick.blueflood.internal;

import com.cloudkick.blueflood.service.Configuration;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
    
    private static HttpServer server;
    private static HttpHandler ackVCKg1rkHandler;
    private static HttpHandler notFoundHandler;
    private static HttpHandler interalErrorHandler;
    protected static ExecutorService httpExecutor;
    private static Boolean isServerUp = false;
    protected static int numUsingServer = 0;
    
    @BeforeClass
    public synchronized static void serverUp() throws IOException {
        if (isServerUp) { 
            virLog("server is already up");
            return; //return since other thread has already spun up the server.
        }
        virLog("spinning up server");
        
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
        
        server = HttpServer.create(new InetSocketAddress(9800), 2); // small backlog.
        isServerUp = true;

        server.createContext(InternalAPIFactory.BASE_PATH + "/accounts/ackVCKg1rk", ackVCKg1rkHandler);
        server.createContext(InternalAPIFactory.BASE_PATH + "/accounts/notFound", notFoundHandler);
        server.createContext(InternalAPIFactory.BASE_PATH + "/accounts/internalError", interalErrorHandler);
        server.createContext(InternalAPIFactory.BASE_PATH + "/test200", ackVCKg1rkHandler);
        server.createContext(InternalAPIFactory.BASE_PATH + "/test404", notFoundHandler);
        server.createContext(InternalAPIFactory.BASE_PATH + "/test500", interalErrorHandler);

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

    @AfterClass
    public synchronized static void serverDown() {
        if (!isServerUp || numUsingServer != 0) {
            virLog("server is already down");
            return; // return since other thread has already stopped the server.
        }
        virLog("powering down");

        server.stop(1);
        isServerUp = false;
        ackVCKg1rkHandler = null;
        server = null;
    }

    protected static void virLog(String s) {
    	System.out.println(String.valueOf(new Date().getTime()) + ", " + s + ", Thread= " + 
    			String.valueOf(Thread.currentThread().getId()));
    }
    
    protected String getClusterString() {
        // Simply using server.getAddress().toString() breaks with some IPv6 issue
        try {
            return InetAddress.getLocalHost().getHostAddress() + ":" + server.getAddress().getPort();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
