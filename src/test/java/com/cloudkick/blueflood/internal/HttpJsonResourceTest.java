package com.cloudkick.blueflood.internal;

import org.apache.http.client.HttpResponseException;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HttpJsonResourceTest extends HttpServerFixture {
    private static final String BASE_PATH = "/test_base";

    JsonResource resource;

    public HttpJsonResourceTest() {
        super(BASE_PATH);
    }

    @Before
    public void setUpResource() {
        ClientConnectionManager connectionManager = InternalAPIFactory.buildConnectionManager(2);
        resource = new HttpJsonResource(connectionManager, getClusterString(), BASE_PATH);
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
