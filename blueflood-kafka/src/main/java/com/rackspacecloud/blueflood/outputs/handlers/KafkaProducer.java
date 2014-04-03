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
import com.rackspacecloud.blueflood.types.IMetric;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KafkaProducer {
    private static final KafkaProducer instance = new KafkaProducer();
    private Producer<String, IMetric> producer = null;
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private KafkaProducer() {

    }

    //Accessor method to get Kafka Instance
    public static synchronized KafkaProducer getInstance() throws IOException {
        if (instance.producer == null) {
            try {
                instance.createProducer();
            }  catch (IOException e){
                log.error("Error encountered while instantiating the Kafka producer", e);
                throw e;
            }
        }

        return instance;
    }

    //Internal method to instantiate the Kafka producer
    private synchronized void createProducer() throws IOException {
        ProducerConfig config = new ProducerConfig(KafkaProducerConfig.asKafkaProperties());
        try {
            producer = new Producer<String, IMetric>(config);
        } catch (Exception e) {
            log.error("Error instantiating producer", e);
            throw new IOException(e);
        }
    }

    public void pushFullResBatch(Collection<IMetric> batch) throws IOException {
        pushBatchToTopic(batch, "metrics");
    }

    public void pushPreaggregatedBatch(Collection<IMetric> batch) throws IOException {
        pushBatchToTopic(batch, "metrics");
    }

    private void pushBatchToTopic(Collection<IMetric> batch, String topic) throws IOException {
        List<KeyedMessage<String, IMetric>> ls = new ArrayList<KeyedMessage<String, IMetric>>();
        for (IMetric iMetric : batch) {
            ls.add(new KeyedMessage<String, IMetric>(topic, iMetric));
        }

        try {
            producer.send(ls);
        } catch (Exception e) {
            log.error("Problem encountered while pushing data to kafka.", e);
            throw new IOException(e);
        }
    }
}
