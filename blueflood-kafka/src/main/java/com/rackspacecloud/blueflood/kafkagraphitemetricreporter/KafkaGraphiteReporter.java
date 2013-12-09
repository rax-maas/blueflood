package com.rackspacecloud.blueflood.kafkagraphitemetricreporter;

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
                        config.getStringProperty(CoreConfig.GRAPHITE_PREFIX)
                );
            } catch (IOException e) {
                log.error("Unable to initialize GraphiteReporter", e);
            }
            if (props.getBoolean("kafka.graphite.metrics.reporter.enabled", false)) {
                initialized = true;
                startReporter(60);
            }
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
        try {
            reporter = new GraphiteReporter(
                    Metrics.defaultRegistry(),
                    config.getStringProperty(CoreConfig.GRAPHITE_HOST),
                    config.getIntegerProperty(CoreConfig.GRAPHITE_PORT),
                    config.getStringProperty(CoreConfig.GRAPHITE_PREFIX)
            );
        } catch (IOException e) {
            log.error("Unable to initialize GraphiteReporter", e);
        }
    }

    @Override
    public String getMBeanName() {
        final String name = String.format("com.rackspacecloud.blueflood.kafkagraphitemetricreporter:type=%s", getClass().getSimpleName());
        return name;
    }
}
