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

package com.rackspacecloud.blueflood.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.Arrays;

public class RouteMatcherTest {
    private RouteMatcher routeMatcher;
    private boolean testRouteHandlerCalled = false;

    @Before
    public void setup() {
        testRouteHandlerCalled = false;
        routeMatcher = new RouteMatcher().withNoRouteHandler(new TestRouteHandler());
    }

    @Test
    public void testNoRouteHandler() throws Exception {
        final HttpRequestHandler dummyHandler = new HttpRequestHandler() {
            @Override
            public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
                // pass
            }
        };

        routeMatcher.get("/", dummyHandler);
        routeMatcher.get("/blah", dummyHandler);

        routeMatcher.route(null, new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/chat"));
        Assert.assertTrue(testRouteHandlerCalled);
    }

    @Test
    public void testValidRouteHandler() throws Exception {
        RouteMatcher router = new RouteMatcher();
        router.get("/", new TestRouteHandler());
        router.route(null, new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
        Assert.assertTrue(testRouteHandlerCalled);
    }
    @Test
    public void testMultiMethodSupport() throws Exception {
        final HttpRequestHandler dummyHandler = new HttpRequestHandler() {
            @Override
            public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
                // pass
            }
        };
        RouteMatcher router = new RouteMatcher();
        router.get("/test/1234/abc", dummyHandler);
        router.post("/test/1234/abc", dummyHandler);
        router.options("/test/1234/abc", dummyHandler);
        Object[] supportedMethods = router.getSupportedMethodsForURL("/test/1234/abc").toArray();
        Arrays.sort(supportedMethods);
        Assert.assertArrayEquals(new String[]{"GET", "OPTIONS", "POST"}, supportedMethods);
    }

    @Test
    public void testValidRoutePatterns() throws Exception {
        FullHttpRequest modifiedReq = testPattern("/metrics/:metricId", "/metrics/foo");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(1, modifiedReq.headers().names().size());
        Assert.assertEquals("metricId", modifiedReq.headers().entries().get(0).getKey());
        Assert.assertEquals("foo", modifiedReq.headers().entries().get(0).getValue());
        testRouteHandlerCalled = false;

        modifiedReq = testPattern("/tenants/:tenantId/entities/:entityId", "/tenants/acFoo/entities/enBar");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(2, modifiedReq.headers().names().size());
        Assert.assertTrue(modifiedReq.headers().get("tenantId").equals("acFoo"));
        Assert.assertTrue(modifiedReq.headers().get("entityId").equals("enBar"));
        testRouteHandlerCalled = false;

        modifiedReq = testPattern("/tenants/:tenantId/entities/:entityId/checks/:checkId/metrics/:metricId/plot",
                "/tenants/acFoo/entities/enBar/checks/chFoo/metrics/myMetric/plot");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(4, modifiedReq.headers().names().size());
        Assert.assertTrue(modifiedReq.headers().get("tenantId").equals("acFoo"));
        Assert.assertTrue(modifiedReq.headers().get("entityId").equals("enBar"));
        Assert.assertTrue(modifiedReq.headers().get("entityId").equals("enBar"));
        Assert.assertTrue(modifiedReq.headers().get("checkId").equals("chFoo"));
        Assert.assertTrue(modifiedReq.headers().get("metricId").equals("myMetric"));
        testRouteHandlerCalled = false;

        modifiedReq = testPattern("/software/:name/:version", "/software/blueflood/v0.1");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(2, modifiedReq.headers().names().size());
        Assert.assertTrue(modifiedReq.headers().get("name").equals("blueflood"));
        Assert.assertTrue(modifiedReq.headers().get("version").equals("v0.1"));
        testRouteHandlerCalled = false;

        // trailing slash
        modifiedReq = testPattern("/software/:name/:version/", "/software/blueflood/v0.1/");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(2, modifiedReq.headers().names().size());
        Assert.assertTrue(modifiedReq.headers().get("name").equals("blueflood"));
        Assert.assertTrue(modifiedReq.headers().get("version").equals("v0.1"));
        testRouteHandlerCalled = false;

        modifiedReq = testPattern("/:name/:version","/blueflood/v0.1");
        Assert.assertTrue(testRouteHandlerCalled);
        Assert.assertEquals(2, modifiedReq.headers().names().size());
        Assert.assertTrue(modifiedReq.headers().get("name").equals("blueflood"));
        Assert.assertTrue(modifiedReq.headers().get("version").equals("v0.1"));
        testRouteHandlerCalled = false;
    }

    private FullHttpRequest testPattern(String pattern, String URI) throws Exception {
        RouteMatcher router = new RouteMatcher();
        final TestRouteHandler handler = new TestRouteHandler();
        // Register handler for pattern
        router.get(pattern, handler);
        // See if handler is called when URI matching pattern is received
        router.route(null, new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, URI));

        // Return modified request (headers might be updated with paramsPositionMap from URI)
        return handler.getRequest();
    }

    private class TestRouteHandler implements HttpRequestHandler {
        private FullHttpRequest request = null;

        @Override
        public void handle(ChannelHandlerContext ctx, FullHttpRequest req) {
            request = req;
            testRouteHandlerCalled = true;
        }

        public FullHttpRequest getRequest() {
            return request;
        }
    }
}