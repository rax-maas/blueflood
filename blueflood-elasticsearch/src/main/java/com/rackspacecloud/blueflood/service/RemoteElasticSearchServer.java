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

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import java.util.List;
import java.net.URL;

public class RemoteElasticSearchServer implements ElasticClientManager {
    private static final RemoteElasticSearchServer INSTANCE = new RemoteElasticSearchServer();

    public static RemoteElasticSearchServer getInstance() {
        return INSTANCE;
    }

    private Client client;

    private RemoteElasticSearchServer() {
        Configuration config = Configuration.getInstance();
        List<String> hosts = config.getListProperty(ElasticIOConfig.ELASTICSEARCH_HOSTS);
        String clusterName = config.getStringProperty(ElasticIOConfig.ELASTICSEARCH_CLUSTERNAME);
        Settings settings;
        Boolean useAuth =  config.getBooleanProperty(ElasticIOConfig.ELASTICSEARCH_USE_AUTH);
        if (useAuth.equals(false)) {
            settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .build();
        } else {
            // Objectrocket docs all use curl -u
            // curl -u base64 encodes username:password and glues it to header: Authorization: Basic
            // Shield from ES does the same or should, saving us the hassle of base64 encoding
            // and setting headers
            String username = config.getStringProperty(ElasticIOConfig.ELASTICSEARCH_USERNAME);
            String passwd = config.getStringProperty(ElasticIOConfig.ELASTICSEARCH_PASSWORD);
            settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("shield.user", username + ":" + passwd)
                    .put("shield:enabled", true) // may not be necessary
                    .build();
        }
        TransportClient tc = new TransportClient(settings);
        // We used to assume hosts looked like
        //      127.0.0.1:9300 or localhost:9300
        // but remote ES using users can have host urls like
        //      http://foo.bar:9300
        for (String host : hosts) {
            String address = null;
            Integer port = null;
            try {
                URL url = new URL(host);
                address = url.getHost() + url.getFile(); // URL<"a.com/foo?a=b">.getFile() => /foo?a=b
                port = url.getPort();
            } catch ( Exception MalformedURLException ) {
                // we tried, default to old ways
                String[] parts = host.split(":");
                address = parts[0];
                port = Integer.parseInt(parts[1]);
            } finally {
                // Hopefully both methods didnt fail but better safe than sorry
                if (address != null && port != null) {
                    tc.addTransportAddress(new InetSocketTransportAddress(address, port));
                }
            }
        }
        client = tc;
    }

    public Client getClient() {
        return client;
    }
}
