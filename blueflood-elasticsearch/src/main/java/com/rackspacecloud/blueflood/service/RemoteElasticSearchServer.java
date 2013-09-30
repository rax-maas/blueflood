package com.rackspacecloud.blueflood.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;


public enum RemoteElasticSearchServer implements ElasticClientManager {
    INSTANCE;

    private Client client;

    private RemoteElasticSearchServer() {
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
