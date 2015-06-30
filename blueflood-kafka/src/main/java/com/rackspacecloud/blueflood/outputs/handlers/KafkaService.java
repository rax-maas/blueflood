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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.eventemitter.Emitter;
import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.eventemitter.RollupEventEmitter;
import com.rackspacecloud.blueflood.outputs.handlers.helpers.KafkaProducerWork;
import com.rackspacecloud.blueflood.service.EventListenerService;
import com.rackspacecloud.blueflood.service.KafkaConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaService implements Emitter.Listener<RollupEvent>, EventListenerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);
    private ArrayList<Producer> producerList = new ArrayList<Producer>();
    private ThreadPoolExecutor kafkaExecutors;
    //Switch to tell if ThreadPool and Kafka Producers were instantiated properly
    private boolean ready = false;
    private static final Integer DEFAULT_KAFKA_PRODUCERS = 5;
    private Integer numberOfProducers;
    private final RollupEventEmitter eventEmitter = RollupEventEmitter.getInstance();
    private final String eventName = RollupEventEmitter.ROLLUP_EVENT_NAME;
    private Random rand = new Random();

    private void init() throws Exception {
        try {
            KafkaConfig config = new KafkaConfig();
            if(config.getBooleanProperty("blueflood.enable.kafka.service")) {
                numberOfProducers = config.getIntegerProperty("blueflood.producer.count") != null ? config.getIntegerProperty("blueflood.producer.count") : DEFAULT_KAFKA_PRODUCERS;
                kafkaExecutors = new ThreadPoolBuilder()
                        .withCorePoolSize(numberOfProducers)
                        .withMaxPoolSize(numberOfProducers)
                        .withUnboundedQueue()
                        .build();
                for(int i=0;i<numberOfProducers;i++) {
                    Producer producer = new Producer(new ProducerConfig(config.getKafkaProperties()));
                    producerList.add(producer);
                }
                ready = true;
            }
        } catch (Exception e) {
            //Takes care of case wherein, initialization threw an exception after thread pool was created
            if(kafkaExecutors != null && !kafkaExecutors.isShutdown()) {
              kafkaExecutors.shutdownNow();
            }
            throw e;
        }
    }

    @Override
    public synchronized void startService() {
        if (!ready) {
            try {
                init();
                if (ready) {
                    //Register with the event emitter
                    eventEmitter.on(eventName, this);
                    log.debug("Listening to event: " + eventName);
                }
            } catch (Exception e) {
                log.error("Could not start Kafka Producer due to errors during initialization phase", e);
            }
            return;
        }
        log.debug("Kafka Production already started for the event: " + eventName);
    }

    @Override
    public synchronized void stopService() {
        //Check to see of the kafka production was already stopped
        if (!eventEmitter.listeners(eventName).contains(this)) {
            log.debug("Kafka Production is already shutdown");
            return;
        }
        //Check if there is some pending work and try to wait for it to complete
        if (!kafkaExecutors.isTerminating() || !kafkaExecutors.isShutdown()) {
            log.debug("Shutting down after terminating all work");
            //Stop the executors
            kafkaExecutors.shutdown();
            //Wait for certain time to terminate thread pool safely.
            try {
                kafkaExecutors.awaitTermination(10,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.debug("Thread interrupted while waiting for safe termination of thread pool executor");
                //Stop the kafka executors abruptly. TODO : Think about the consequences?
                kafkaExecutors.shutdownNow();
            }
        }
        //Un-subscribe from event emitter
        eventEmitter.off(this.eventName, this);
        //Not really required, but considers an impossible case of someone calling loadAndStart on 'stopped' KafkaService instance
        ready = false;
        log.debug("Stopped listening to event: " + this.eventName);
    }

    @Override
    public void call(RollupEvent... rollupPayload) {
        kafkaExecutors.execute(new KafkaProducerWork(producerList.get(rand.nextInt(numberOfProducers)), rollupPayload));
    }

    //Used only for tests
    public ThreadPoolExecutor getKafkaExecutorsUnsafe() {
        return kafkaExecutors;
    }

    //Used only for tests
    public ArrayList<Producer> getProducerListUnsafe() {
        return producerList;
    }
}
