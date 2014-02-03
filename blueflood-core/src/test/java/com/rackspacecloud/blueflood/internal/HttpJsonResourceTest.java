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

import org.apache.http.client.HttpResponseException;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;

@Ignore
public class HttpJsonResourceTest {
    private static final String BASE_PATH = "/test_base";

    ClientConnectionManager connectionManager;
    JsonResource resource;
    HttpServerFixture server;

    @Before
    public void setUpResource() throws IOException {
        server = new HttpServerFixture(BASE_PATH, 9800);
        server.serverUp();
        connectionManager = InternalAPIFactory.buildConnectionManager(2);
        resource = new HttpJsonResource(connectionManager, server.getClusterString(), BASE_PATH);
    }
    
    @After
    public void serverDown() {
        server.serverDown();
        if (connectionManager != null)
            connectionManager.shutdown();
    }

    @Test
    public void test200() throws IOException {
        resource.getResource("/test200");
    }

    @Test
    public void test404() throws IOException {
        try {
            resource.getResource("/test404");
            Assert.fail("should not have succeeded");
        } catch (HttpResponseException ex) {
            Assert.assertEquals(404, ex.getStatusCode());
        }
    }

    @Test
    public void test500() throws IOException {
        try {
            resource.getResource("/test500");
            Assert.fail("should not have succeeded");
        } catch (HttpResponseException ex) {
            Assert.assertEquals(500, ex.getStatusCode());
        }
    }

    // similar to test in InternalAPITest.
    @Test
    public void testClusterException() throws IOException {
        try {
            ClientConnectionManager connectionManager = InternalAPIFactory.buildConnectionManager(2);
            resource = new HttpJsonResource(connectionManager, "127.0.0.1:3232,127.0.0.1:3232,192.0.2.1:3232,192.0.2.2:3232", BASE_PATH);
            resource.getResource("/test200");
            Assert.fail("should not have succeeded");
        } catch (ClusterException ex) {
            Assert.assertEquals(3, ex.size());
            Assert.assertTrue(ex.getException("127.0.0.1:3232") instanceof HttpHostConnectException);
            Assert.assertTrue(ex.getException("192.0.2.1:3232") instanceof ConnectTimeoutException);
            Assert.assertTrue(ex.getException("192.0.2.2:3232") instanceof ConnectTimeoutException);
        } finally {
            connectionManager.shutdown();
        }
    }

    @Test
    public void testStringArrayIterationOrder() {
        String[] cluster = new String[] {"c", "d", "b", "a", "z"};
        StringBuilder sb = new StringBuilder();
        for (String host : cluster)
            sb = sb.append(host);
        Assert.assertEquals("cdbaz", sb.toString());
    }
}
