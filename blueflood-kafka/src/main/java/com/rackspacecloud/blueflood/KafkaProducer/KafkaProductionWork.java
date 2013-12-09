/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.kafkaproducer;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

class KafkaProductionWork implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaProductionWork.class);
    private RollupEvent[] rollupEventObjects;
    private Producer producer;

    KafkaProductionWork(Producer producer, RollupEvent... objects) {
        this.rollupEventObjects = objects;
        this.producer = producer;
    }

    @Override
    public void run() {
        //TODO : Generalize this later to work for any event and not just rollup
        ArrayList<KeyedMessage<String, RollupEvent>> messages = new ArrayList<KeyedMessage<String, RollupEvent>>();
        for (RollupEvent rollupEvent : rollupEventObjects) {
            messages.add(new KeyedMessage<String, RollupEvent>(rollupEvent.getGranularityName(), rollupEvent));
        }
        log.debug("Sending messages to producer "+producer.toString());
        try {
            producer.send(messages);
        } catch (Exception e) {
            log.error("Error encountered while sending messages using Kafka Producer", e);
        }
    }
 }

