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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.http.client.HttpResponseException;

import org.apache.http.conn.ClientConnectionManager;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
public class InternalApiIntegrationTest {
    private final String clusterString = Configuration.getInstance().getStringProperty(CoreConfig.INTERNAL_API_CLUSTER);

    @Test
    public void testRegularWorks() throws IOException {
        InternalAPI api = InternalAPIFactory.create(5,  clusterString);
        try {
            Account acct = api.fetchAccount("acDUSBABEK");
            Assert.assertNotNull(acct);
        } catch (IOException ex) {
            if (ex instanceof ClusterException) {
                ClusterException clusterEx = (ClusterException)ex;
                for (Throwable err : clusterEx.getExceptions())
                    err.printStackTrace();
            } else
                ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            shutdown(api);
        }
    }

    @Test(expected = HttpResponseException.class)
    public void testRegularHttpError() throws IOException {
        InternalAPI api = InternalAPIFactory.create(1, clusterString);
        Account acct = api.fetchAccount("acBOGUS___");
        shutdown(api);
    }

    @Test
    public void testConcurrentRequests() throws InterruptedException {
        final int threads = 150;
        final InternalAPI api = InternalAPIFactory.create(5, clusterString);
        final AtomicInteger errors = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread("request " + i) {
                public void run() {
                    try {
                        Account acct = api.fetchAccount("acDUSBABEK");
                        Assert.assertNotNull(acct);
                    } catch (IOException ex) {
                        errors.incrementAndGet();
                        ex.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            }.start();
        }
        latch.await(5000, TimeUnit.MILLISECONDS);
        shutdown(api);
        if (errors.get() > 0)
            Assert.fail("There were IOExceptions on some requests.");
    }

    private static void shutdown(InternalAPI api) {
        ClientConnectionManager connectionManager = (ClientConnectionManager)Whitebox.getInternalState(api, "connectionManager");
        connectionManager.shutdown();
    }
}
