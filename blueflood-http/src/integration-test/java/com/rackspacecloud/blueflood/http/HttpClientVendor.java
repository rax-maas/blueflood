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

import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;

public class HttpClientVendor {
    private DefaultHttpClient client;

    public HttpClientVendor() {
        client = new DefaultHttpClient(buildConnectionManager(20));
        client.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, true);
        client.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
        client.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 30000);

        // Wait this long for an available connection. Setting this correctly is important in order to avoid
        // connectionpool timeouts.
        client.getParams().setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, 5000);
    }
    
    public DefaultHttpClient getClient() {
        return client;
    }

    private ClientConnectionManager buildConnectionManager(int concurrency) {
        final PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(concurrency);
        connectionManager.setMaxTotal(concurrency);
        return connectionManager;
    }

    public void shutdown() {
        if (client != null) {
            client.getConnectionManager().shutdown();
        }
    }
}
