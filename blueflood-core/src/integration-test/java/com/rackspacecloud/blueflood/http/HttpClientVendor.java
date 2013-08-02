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
