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

package com.rackspacecloud.blueflood.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);
    private Properties props = new Properties();

    public KafkaConfig() {
        try {
            init();
        } catch (IOException ex) {
            log.error("Error encountered while loading the Kafka Config", ex);
            throw new RuntimeException(ex);
        }
    }

    private void init() throws IOException {
        //Load the configuration
        String configStr = System.getProperty("kafka.config");
        if (configStr != null) {
            URL configUrl = new URL(configStr);
            props.load(configUrl.openStream());
        }
    }

    public Properties getKafkaProperties() {
        return props;
    }

    public String getStringProperty(String name) {
        return props.getProperty(name);
    }

    public Integer getIntegerProperty(String propertyName) {
        return Integer.parseInt(getStringProperty(propertyName));
    }

    public boolean getBooleanProperty(String name) {
        return getStringProperty(name).equalsIgnoreCase("true");
    }
}
