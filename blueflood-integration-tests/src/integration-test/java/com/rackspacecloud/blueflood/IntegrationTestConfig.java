package com.rackspacecloud.blueflood;

import com.rackspacecloud.blueflood.service.ConfigDefaults;
import com.rackspacecloud.blueflood.service.Configuration;

public enum IntegrationTestConfig implements ConfigDefaults {

    /**
     * Sets the method of starting Elasticsearch for integration tests. This will change over time as we update
     * Elasticsearch and change to new testing mechanisms (i.e. we drop TLRX and adopt TEST_CONTAINERS). Valid values:
     *
     * TLRX: Default value and the old way of testing against Elasticsearch used elsewhere in the project. It seems to
     * start an in-memory instance. This library hasn't been maintained in years, so we need to stop using it for
     * testing new versions of Elasticsearch.
     *
     * TEST_CONTAINERS: Probably the best way to test against Elasticsearch for the foreseeable future. Testcontainers
     * is a library that provides many on-demand services for testing purposes via Docker. It officially supports
     * Elasticsearch 5.4.0 and later, but older version work with it, too.
     *
     * EXTERNAL: Indicates that you'll start Elasticsearch externally, like with Docker. See the 10-minute guide on the
     * wiki for a quick startup. The test framework will do nothing in terms of starting or initializing Elasticsearch
     * when you use this option.
     */
    IT_ELASTICSEARCH_TEST_METHOD("TEST_CONTAINERS"),
    /**
     * Only when IT_ELASTICSEARCH_TEST_METHOD == 'TEST_CONTAINERS', this sets the Elasticsearch version to run
     * integration tests with. This should be the version of an available Docker image from DockerHub.
     */
    IT_ELASTICSEARCH_CONTAINER_VERSION("6.8.23");

    static {
        Configuration.getInstance().loadDefaults(IntegrationTestConfig.values());
    }

    private String defaultValue;

    private IntegrationTestConfig(String value) {
        this.defaultValue = value;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
