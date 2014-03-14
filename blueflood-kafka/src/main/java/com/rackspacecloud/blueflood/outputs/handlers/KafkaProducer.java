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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.types.MetricsCollection;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class KafkaProducer {
    private static final KafkaProducer instance = new KafkaProducer();
    private static Producer<String, MetricsCollection> producer;
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private KafkaProducer() {

    }

    private static final Properties configProperties;

    static {
        configProperties = new Properties();

        configProperties.put("metadata.broker.list", "localhost:9092");
        configProperties.put("serializer.class", "com.rackspacecloud.blueflood.io.serializers.MetricsBatchSerializer");
        configProperties.put("producer.type", "sync");
        configProperties.put("request.required.acks", "1");
    }

    //Accessor method to get Kafka Instance
    public static KafkaProducer getInstance() throws IOException {
        if (producer == null){
            try {
                getProducer();
            }  catch (IOException e){
                log.error("Error encountered while instantiating the Kafka producer");
                throw e;
            }
        }

        return instance;
    }

    //Internal method to instantiate the Kafka producer
    private static void getProducer() throws IOException {
        ProducerConfig config = new ProducerConfig(configProperties);

        producer = new Producer<String, MetricsCollection>(config);
    }

    public void pushFullResBatch(MetricsCollection batch) {
        KeyedMessage<String, MetricsCollection> data = new KeyedMessage<String, MetricsCollection>("metrics_full", batch);
        System.out.println("about to send some data");

        try {producer.send(data);} catch (Exception e) {
            System.out.println("GOT AN EXCEPITON SENDING");
            e.printStackTrace();
            System.out.println(e);
        }
        System.out.println("Sent that data");
    }
}
