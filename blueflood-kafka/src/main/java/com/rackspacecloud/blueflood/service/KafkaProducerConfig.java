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

public enum KafkaProducerConfig implements ConfigDefaults {
    // see http://kafka.apache.org/08/configuration.html
    KAFKA_METADATA_BROKER_LIST("localhost:9092"),
    KAFKA_REQUEST_REQUIRED_ACKS("1"),
    KAFKA_REQUEST_TIMEOUT_MS("10000"),
    KAFKA_PRODUCER_TYPE("sync"),
    KAFKA_SERIALIZER_CLASS("com.rackspacecloud.blueflood.io.serializers.IMetricKafkaSerializer"),
    KAFKA_KEY_SERIALIZER_CLASS("kafka.serializer.StringEncoder"),
    KAFKA_PARTITIONER_CLASS("kafka.producer.DefaultPartitioner"),
    KAFKA_COMPRESSION_CODEC("none"),
    KAFKA_COMPRESSED_TOPICS("null"),
    KAFKA_MESSAGE_SEND_MAX_RETRIES("3"),
    KAFKA_RETRY_BACKOFF_MS("100"),
    KAFKA_TOPIC_METADATA_REFRESH_INTERVAL_MS("600000"),
    KAFKA_QUEUE_BUFFERING_MAX_MS("5000"),
    KAFKA_QUEUE_BUFFERING_MAX_MESSAGES("10000"),
    KAFKA_QUEUE_ENQUEUE_TIMEOUT_MS("-1"),
    KAFKA_BATCH_NUM_MESSAGES("200"),
    KAFKA_SEND_BUFFER_BYTES("102400"),
    KAFKA_CLIENT_ID("");

    static {
        Configuration.getInstance().loadDefaults(KafkaProducerConfig.values());
    }

    private String defaultValue;
    private KafkaProducerConfig(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }

    public static Properties asKafkaProperties() {
        Properties p = new Properties();
        Configuration conf = Configuration.getInstance();
        p.setProperty("metadata.broker.list", conf.getStringProperty(KAFKA_METADATA_BROKER_LIST));
        p.setProperty("request.required.acks", conf.getStringProperty(KAFKA_REQUEST_REQUIRED_ACKS));
        p.setProperty("request.timeout.ms", conf.getStringProperty(KAFKA_REQUEST_TIMEOUT_MS));
        p.setProperty("producer.type", conf.getStringProperty(KAFKA_PRODUCER_TYPE));
        p.setProperty("serializer.class", conf.getStringProperty(KAFKA_SERIALIZER_CLASS));
        p.setProperty("key.serializer.class", conf.getStringProperty(KAFKA_KEY_SERIALIZER_CLASS));
        p.setProperty("partitioner.class", conf.getStringProperty(KAFKA_PARTITIONER_CLASS));
        p.setProperty("compression.codec", conf.getStringProperty(KAFKA_COMPRESSION_CODEC));
        p.setProperty("compressed.topics", conf.getStringProperty(KAFKA_COMPRESSED_TOPICS));
        p.setProperty("message.send.max.retries", conf.getStringProperty(KAFKA_MESSAGE_SEND_MAX_RETRIES));
        p.setProperty("retry.backoff.ms", conf.getStringProperty(KAFKA_RETRY_BACKOFF_MS));
        p.setProperty("topic.metadata.refresh.interval_ms", conf.getStringProperty(KAFKA_TOPIC_METADATA_REFRESH_INTERVAL_MS));
        p.setProperty("queue.buffering.max.ms", conf.getStringProperty(KAFKA_QUEUE_BUFFERING_MAX_MS));
        p.setProperty("queue.buffering.max.messages", conf.getStringProperty(KAFKA_QUEUE_BUFFERING_MAX_MESSAGES));
        p.setProperty("queue.enqueue.timeout.ms", conf.getStringProperty(KAFKA_QUEUE_ENQUEUE_TIMEOUT_MS));
        p.setProperty("batch.num.messages", conf.getStringProperty(KAFKA_BATCH_NUM_MESSAGES));
        p.setProperty("send.buffer.bytes", conf.getStringProperty(KAFKA_SEND_BUFFER_BYTES));
        p.setProperty("client.id", conf.getStringProperty(KAFKA_CLIENT_ID));
        return p;
    }
}
