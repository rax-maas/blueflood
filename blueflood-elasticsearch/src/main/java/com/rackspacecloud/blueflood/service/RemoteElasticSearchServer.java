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
import org.elasticsearch.node.NodeBuilder;


public class RemoteElasticSearchServer implements ElasticClientManager {
    private static final RemoteElasticSearchServer INSTANCE = new RemoteElasticSearchServer();

    public static RemoteElasticSearchServer getInstance() {
        return INSTANCE;
    }

    private Client client;

    protected RemoteElasticSearchServer() {
        if (Configuration.getBooleanProperty("USE_ELASTICSEARCH")) {
            String host = Configuration.getStringProperty("ELASTICSEARCH_HOST");
            Integer port = Configuration.getIntegerProperty("ELASTICSEARCH_PORT");
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("client.transport.ignore_cluster_name", true)
                    .build();
            client = new TransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(host, port));
        }
    }

    @Override
    public Client getClient() {
        if (!Configuration.getBooleanProperty("USE_ELASTICSEARCH")) {
            throw new UnsupportedOperationException("Configured not to use ES.");
        }
        return client;
    }
}
