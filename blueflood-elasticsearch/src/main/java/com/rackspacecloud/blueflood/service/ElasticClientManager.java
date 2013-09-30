package com.rackspacecloud.blueflood.service;

import org.elasticsearch.client.Client;

public interface ElasticClientManager {

    public Client getClient();

}
