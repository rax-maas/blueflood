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

import com.rackspacecloud.blueflood.service.KafkaProducerConfig;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaProducer {
    private static final KafkaProducer instance = new KafkaProducer();
    private static Producer<String, MetricsCollection> producer;
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private KafkaProducer() {

    }

    //Accessor method to get Kafka Instance
    public static synchronized KafkaProducer getInstance() throws IOException {
        if (producer == null) {
            try {
                createProducer();
            }  catch (IOException e){
                log.error("Error encountered while instantiating the Kafka producer");
                throw e;
            }
        }

        return instance;
    }

    //Internal method to instantiate the Kafka producer
    private static synchronized void createProducer() throws IOException {
        ProducerConfig config = new ProducerConfig(KafkaProducerConfig.asKafkaProperties());

        producer = new Producer<String, MetricsCollection>(config);
    }

    public void pushFullResBatch(MetricsCollection batch) throws IOException {
        KeyedMessage<String, MetricsCollection> data = new KeyedMessage<String, MetricsCollection>("metrics_full", batch);
        try {
            producer.send(data);
        } catch (Exception e) {
            log.error("Problem encountered while pushing data to kafka.", e);
            throw new IOException(e);
        }
    }
}
