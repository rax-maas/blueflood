package com.rackspacecloud.blueflood.kafka;


import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wrapper class for the Kafka producer
 */

public class KafkaProducer {

    private static final KafkaProducer instance = new KafkaProducer();
    private static Producer<String,String> producer;
    private static final String kafkaProducerDelimiter = ",";
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private KafkaProducer() {

    }

    //Accessor method to get Kafka Instance
    public static KafkaProducer getInstance() throws IOException {

        if(producer == null){
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

        ProducerConfig config = new ProducerConfig(new KafkaProducerConfig().getProperties());
        producer = new Producer<String, String>(config);

    }

    //Thread-safe method to push a roll-up using Kafka producer
    public synchronized void pushRollup(Locator locator, Range range, Rollup rollup) throws Exception {

        KeyedMessage<String,String> data = new KeyedMessage<String,String>(locator.getTenantId(),range+kafkaProducerDelimiter+rollup);

        log.debug("Pushing roll-up to Kafka: " + data);

        try{
        producer.send(data);
        } catch(Exception e){
            log.error("Error encountered while pushing roll-up to Kafka: " + data);
            throw e;
        }
    }
}
