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
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class InternalApiTest {
    private InternalAPI api;
    private HttpServerFixture server;

    public InternalApiTest() {
    }

    @Before
    public void setupApi() throws Exception {
        server = new HttpServerFixture((InternalAPIFactory.BASE_PATH), 9801);
        server.serverUp();
        api = InternalAPIFactory.create(2, server.getClusterString());
    }

    @After
    public void cleanUpApi() {
        server.serverDown();
        ClientConnectionManager connectionManager = (ClientConnectionManager)Whitebox.getInternalState(api, "connectionManager");
        if (connectionManager != null)
            connectionManager.shutdown();
    }
    
    @Test
    public void testGetAccount() throws IOException {
        Account acct = api.fetchAccount("ackVCKg1rk");
        Assert.assertEquals("ackVCKg1rk", acct.getId());
    }
    
    @Test(expected = HttpResponseException.class)
    public void testBadAccount() throws IOException {
        api.fetchAccount("notFound");
    }
    
    @Test(expected = HttpResponseException.class)
    public void testInternalError() throws IOException {
        api.fetchAccount("internalError");
    }
    
    @Test(expected = HttpResponseException.class) // a 404 in this case.
    public void testNoRoute() throws IOException {
        api.fetchAccount("noRoute");
    }
    
    @Test
    public void testConnectionRefused() throws IOException {
        try {
            InternalAPI badApi = InternalAPIFactory.create(5, "127.0.0.1:3232,127.0.0.1:3232,192.0.2.1:3232,192.0.2.2:3232");
            badApi.fetchAccount("bogus-tenant-id");
            Assert.fail("fetchAccount should have failed");
        } catch (ClusterException ex) {
            Assert.assertEquals(3, ex.size());
            Assert.assertTrue(ex.getException("127.0.0.1:3232") instanceof HttpHostConnectException);
            Assert.assertTrue(ex.getException("192.0.2.1:3232") instanceof ConnectTimeoutException);
            Assert.assertTrue(ex.getException("192.0.2.2:3232") instanceof ConnectTimeoutException);
        }
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        // rebuild the connection with a 
        int numThreads = 150;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        for (int i = 0; i < numThreads; i++) {
            new Thread("concurrent " + i) { 
                public void run() {
                    try {
                        Assert.assertEquals("ackVCKg1rk", api.fetchAccount("ackVCKg1rk").getId());
                        latch.countDown();
                    } catch (ClusterException ex) {
                        errors.incrementAndGet();
                        ex.getExceptions().iterator().next().printStackTrace();
                    } catch (IOException ex) {
                        errors.incrementAndGet();
                    }
                }
            }.start();
        }
        latch.await(5000, TimeUnit.MILLISECONDS);
        if (errors.get() > 0)
            Assert.fail("There were IOExceptions with concurrent connections");
    }
    
    @Test(expected = SocketTimeoutException.class)
    public void testConnectionTimeout() throws Throwable {
        final CountDownLatch waitLatch = new CountDownLatch(1);
        ExecutorService httpExecutor = (ExecutorService)Whitebox.getInternalState(server, "httpExecutor");
        httpExecutor.submit(new Runnable() {
            public void run() {
                try {
                    waitLatch.await(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) { }
            }
        });
        
        // server should block requests for next 5sec. force timeout to something short, then make a request.
        // api.jsonResource.client
        DefaultHttpClient client = new DefaultHttpClient(InternalAPIFactory.buildConnectionManager(2));
        client.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, true);
        client.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 100);
        client.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 100);
        Whitebox.setInternalState(Whitebox.getInternalState(api, "jsonResource"), "client", client);
        
        try {
            api.fetchAccount("ackVCKg1rk");
            Assert.fail("Should not have fetched account");
        } catch (ClusterException ex) {
            Assert.assertEquals(1, ex.size());
            throw ex.getExceptions().iterator().next();
        } finally {
            waitLatch.countDown();
            Thread.sleep(500);
        }
    }
}
