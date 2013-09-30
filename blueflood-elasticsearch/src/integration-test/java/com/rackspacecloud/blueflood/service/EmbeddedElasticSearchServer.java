package com.rackspacecloud.blueflood.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeBuilder;

import com.rackspacecloud.blueflood.io.ElasticIO;

public enum EmbeddedElasticSearchServer implements ElasticClientManager{
    INSTANCE;

    private Client client;

    private EmbeddedElasticSearchServer() {
        client = NodeBuilder.nodeBuilder().node().client();
        // should we perform cleanup and/or initialization here?
        deleteIndices();
        initIndices();
    }

    private void deleteIndices() {
        client.admin().cluster()
            .prepareHealth()
            .setWaitForYellowStatus()
            .execute().actionGet();
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    private void initIndices() {
        final int numIndices = 1;
        for(int index = 0; index<numIndices; index++) {
            client.admin().indices()
                .prepareCreate(ElasticIO.INDEX_PREFIX+index)
                .execute().actionGet();
            // this will block until clusterhealth is yellow.
            client.admin().cluster()
                    .prepareHealth()
                    .setWaitForYellowStatus()
                    .execute().actionGet();
            client.admin().indices().prepareRefresh().execute().actionGet();
        }
    }

    @Override
    public Client getClient() {
        return client;
    }
}
