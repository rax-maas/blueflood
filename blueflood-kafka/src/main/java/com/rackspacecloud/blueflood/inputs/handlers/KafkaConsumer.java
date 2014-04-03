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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.io.serializers.IMetricKafkaSerializer;
import com.rackspacecloud.blueflood.service.KafkaConsumerConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumer {
    private static final KafkaConsumer instance = new KafkaConsumer();
    private ConsumerConnector consumer = null;
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private KafkaStream<String, IMetric> stream;

    private KafkaConsumer() {}

    public static synchronized KafkaConsumer getInstance() {
        if (instance.consumer == null) {
            instance.createConsumer();
        }
        return instance;
    }

    public KafkaStream<String, IMetric> getStream() {
        return stream;
    }

    public void commitOffsets() { consumer.commitOffsets(); }

    private synchronized void createConsumer() {
        ConsumerConfig config = new ConsumerConfig(KafkaConsumerConfig.asKafkaProperties());

        consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicToCountMap = new HashMap<String, Integer>();
        // there is no fine-grained control over offsets on a per-stream basis.
        // you risk data loss when using the high level consumer with multiple separate streams.
        // once kafka 0.9 comes out there should be a better solution.
        // before that, the best option for improving perf is to rewrite all the consumer code using Kafka Simple Consumer
        topicToCountMap.put("metrics", 1);
        IMetricKafkaSerializer serializer = new IMetricKafkaSerializer(new VerifiableProperties());

        stream = consumer.createMessageStreams(topicToCountMap, new StringDecoder(new VerifiableProperties()), serializer)
                .get("metrics").get(0);
    }
}
