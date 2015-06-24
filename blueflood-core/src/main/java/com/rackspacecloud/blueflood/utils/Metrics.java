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


import com.codahale.metrics.*;
import com.codahale.metrics.riemann.Riemann;
import com.codahale.metrics.riemann.RiemannReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.log4j.InstrumentedAppender;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.log4j.LogManager;

import javax.management.MBeanServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Metrics {
    private static final MetricRegistry registry = new MetricRegistry();
    private static final GraphiteReporter reporter;
    private static final RiemannReporter reporter1;
    private static final JmxReporter reporter2;
    private static final String JVM_PREFIX = "jvm";

    static {
        Configuration config = Configuration.getInstance();

        // register jvm metrics
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (!System.getProperty("java.version").split("\\.")[1].equals("6")) {
            // if not running 1.6
            registry.registerAll(new PrefixedMetricSet(new BufferPoolMetricSet(mbs), JVM_PREFIX, "buffer-pool"));
        }
        registry.registerAll(new PrefixedMetricSet(new GarbageCollectorMetricSet(), JVM_PREFIX, "gc"));
        registry.registerAll(new PrefixedMetricSet(new MemoryUsageGaugeSet(), JVM_PREFIX, "memory"));
        registry.registerAll(new PrefixedMetricSet(new ThreadStatesGaugeSet(), JVM_PREFIX, "thread-states"));

        // instrument log4j
        InstrumentedAppender appender = new InstrumentedAppender(registry);
        appender.activateOptions();
        LogManager.getRootLogger().addAppender(appender);

        if (!config.getStringProperty(CoreConfig.RIEMANN_HOST).equals("")) {
            RiemannReporter tmpreporter;
            try {
                Riemann riemann = new Riemann(config.getStringProperty(CoreConfig.RIEMANN_HOST), config.getIntegerProperty(CoreConfig.RIEMANN_PORT));

                RiemannReporter.Builder builder = RiemannReporter
                        .forRegistry(registry)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .convertRatesTo(TimeUnit.SECONDS);
                if (!config.getStringProperty(CoreConfig.RIEMANN_SEPARATOR).isEmpty()) {
                    builder.useSeparator(config.getStringProperty(CoreConfig.RIEMANN_SEPARATOR));
                }
                if (!config.getStringProperty(CoreConfig.RIEMANN_TTL).isEmpty()) {
                    builder.withTtl(config.getFloatProperty(CoreConfig.RIEMANN_TTL));
                }
                if (!config.getStringProperty(CoreConfig.RIEMANN_LOCALHOST).isEmpty()) {
                    builder.localHost(config.getStringProperty(CoreConfig.RIEMANN_LOCALHOST));
                }
                if (!config.getStringProperty(CoreConfig.RIEMANN_PREFIX).isEmpty()) {
                    builder.prefixedWith(config.getStringProperty(CoreConfig.RIEMANN_PREFIX));
                }
                if (!config.getStringProperty(CoreConfig.RIEMANN_TAGS).isEmpty()) {
                    builder.tags(config.getListProperty(CoreConfig.RIEMANN_TAGS));
                }
                tmpreporter = builder.build(riemann);

                tmpreporter.start(30l, TimeUnit.SECONDS);
            } catch (IOException e) {
                tmpreporter = null;
            }
            reporter1 = tmpreporter;
        } else {
            reporter1 = null;
        }

        if (!config.getStringProperty(CoreConfig.GRAPHITE_HOST).equals("")) {
            Graphite graphite = new Graphite(new InetSocketAddress(config.getStringProperty(CoreConfig.GRAPHITE_HOST), config.getIntegerProperty(CoreConfig.GRAPHITE_PORT)));

            reporter = GraphiteReporter
                    .forRegistry(registry)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .prefixedWith(config.getStringProperty(CoreConfig.GRAPHITE_PREFIX))
                    .build(graphite);

            reporter.start(30l, TimeUnit.SECONDS);
        } else {
            reporter = null;
        }

        reporter2 = JmxReporter
                .forRegistry(registry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();
        reporter2.start();
    }

    static class PrefixedMetricSet implements MetricSet {
        private final Map<String, Metric> metricMap;

        PrefixedMetricSet(final MetricSet metricSet, final String prefix1, final String prefix2) {
            metricMap = Collections.unmodifiableMap(new HashMap<String, Metric>(){{
                for (Map.Entry<String, Metric> stringMetricEntry : metricSet.getMetrics().entrySet()) {
                    put(MetricRegistry.name(prefix1, prefix2, stringMetricEntry.getKey()), stringMetricEntry.getValue());
                }
            }});
        }

        @Override
        public Map<String, Metric> getMetrics() {
            return metricMap;
        }
    }

    public static MetricRegistry getRegistry() {
        return registry;
    }

    public static Meter meter(Class kls, String... names) {
        return getRegistry().meter(MetricRegistry.name(kls, names));
    }

    public static Timer timer(Class kls, String... names) {
        return getRegistry().timer(MetricRegistry.name(kls, names));
    }

    public static Histogram histogram(Class kls, String... names) {
        return getRegistry().histogram(MetricRegistry.name(kls, names));
    }

    public static Counter counter(Class kls, String... names) {
        return getRegistry().counter(MetricRegistry.name(kls, names));
    }
}
