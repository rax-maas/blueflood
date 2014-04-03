/*
 * Copyright 2014 Rackspace
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

import java.util.Properties;

public enum KafkaConsumerConfig implements ConfigDefaults {
    INGEST_THREADS("5"),
    KAFKA_CONSUMER_MAX_BATCH_SIZE("200"),
    KAFKA_GROUP_ID("blueflood"),
    KAFKA_ZOOKEEPER_CONNECT("localhost:2181"),
    // you probably don't want to modify auto-commit unless you know what you are doing.
    // KafkaConsumerWorker handles commiting of offsets after things have been persisted.
    KAFKA_AUTO_COMMIT_ENABLE("false"),
    KAFKA_CONSUMER_TIMEOUT_MS("100");
    // TODO: expand to include all values in http://kafka.apache.org/08/configuration.html

    static {
        Configuration.getInstance().loadDefaults(KafkaConsumerConfig.values());
    }

    private String defaultValue;
    private KafkaConsumerConfig(String value) {
        this.defaultValue = value;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
    public static Properties asKafkaProperties() {
        Properties p = new Properties();
        Configuration config = Configuration.getInstance();
        p.setProperty("group.id", config.getStringProperty(KafkaConsumerConfig.KAFKA_GROUP_ID));
        p.setProperty("zookeeper.connect", config.getStringProperty(KafkaConsumerConfig.KAFKA_ZOOKEEPER_CONNECT));
        p.setProperty("auto.commit.enable", config.getStringProperty(KafkaConsumerConfig.KAFKA_AUTO_COMMIT_ENABLE));
        p.setProperty("consumer.timeout.ms", config.getStringProperty(KafkaConsumerConfig.KAFKA_CONSUMER_TIMEOUT_MS));
        return p;
    }
}
