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
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();
        TransportClient tc = new TransportClient(settings);
        for (String host : hosts) {
            String[] parts = host.split(":");
            String address = parts[0];
            Integer port = Integer.parseInt(parts[1]);
            tc.addTransportAddress(new InetSocketTransportAddress(address, port));
        }
        client = tc;
    }

    public Client getClient() {
        return client;
    }
}
