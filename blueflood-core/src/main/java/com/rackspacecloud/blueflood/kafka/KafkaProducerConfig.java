package com.rackspacecloud.blueflood.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 This is a package-private class used to get the Kafka producer properties
 */
class KafkaProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);
    private static final String kafkaProducerConfigPath = "kafkaProducerConfig.properties";

    Properties getProperties() throws IOException {

        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream(kafkaProducerConfigPath);
        try {
            props.load(in);
        } catch (IOException e) {
            log.error("Failed to load the Kafka producer configuration from " + kafkaProducerConfigPath);
            throw e;
        } finally {
            in.close();
        }

        return props;
    }

}
