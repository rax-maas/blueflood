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

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.reporting.GraphiteReporter;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KafkaGraphiteReporter implements KafkaMetricsReporter, KafkaGraphiteReporterMBean {
    private static final Logger log = Logger.getLogger(KafkaGraphiteReporter.class);
    GraphiteReporter reporter;
    Configuration config = Configuration.getInstance();
    boolean initialized = false;
    boolean running = false;

    @Override
    public void init(VerifiableProperties props) {
        if (!initialized && !config.getStringProperty(CoreConfig.GRAPHITE_HOST).equals("")) {
            try {
                reporter = new GraphiteReporter(
                        Metrics.defaultRegistry(),
                        config.getStringProperty(CoreConfig.GRAPHITE_HOST),
                        config.getIntegerProperty(CoreConfig.GRAPHITE_PORT),
                        config.getStringProperty(CoreConfig.GRAPHITE_PREFIX + "kafka.")
                );
            } catch (IOException e) {
                log.error("Unable to initialize GraphiteReporter", e);
            }
            initialized = true;
            startReporter(30);
        }
    }


    @Override
    public void startReporter(long pollingInterval) {
        if (initialized && !running) {
            reporter.start(pollingInterval, TimeUnit.SECONDS);
            running = true;
            log.info(String.format("Started Kafka Graphite metrics reporter with polling period %d seconds", pollingInterval));
        }
    }

    @Override
    public void stopReporter() {
        if (initialized && running) {
            reporter.shutdown();
            running = false;
            log.info("Stopped Kafka Graphite metrics reporter");
        }
    }

    @Override
    public String getMBeanName() {
        final String name = String.format("com.rackspacecloud.blueflood.kafkagraphitemetricreporter:type=%s", getClass().getSimpleName());
        return name;
    }
}
