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
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class Metrics {
    private static final MetricRegistry registry = new MetricRegistry();
    private static final GraphiteReporter reporter;
    private static final JmxReporter reporter2;

    static {
        Configuration config = Configuration.getInstance();

        // register jvm metrics
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (!System.getProperty("java.version").split("\\.")[1].equals("6")) {
            // if not running 1.6
            registry.registerAll(new BufferPoolMetricSet(mbs));
        }
        registry.registerAll(new GarbageCollectorMetricSet());
        registry.registerAll(new MemoryUsageGaugeSet());
        registry.registerAll(new ThreadStatesGaugeSet());

        // instrument log4j
        InstrumentedAppender appender = new InstrumentedAppender(registry);
        appender.activateOptions();
        LogManager.getRootLogger().addAppender(appender);

        if (!config.getStringProperty(CoreConfig.GRAPHITE_HOST).equals("")) {
            Graphite graphite = new Graphite(new InetSocketAddress(config.getStringProperty(CoreConfig.GRAPHITE_HOST), config.getIntegerProperty(CoreConfig.GRAPHITE_PORT)));

            reporter = GraphiteReporter
                    .forRegistry(registry)
                    .convertDurationsTo(TimeUnit.SECONDS)
                    .convertRatesTo(TimeUnit.MILLISECONDS)
                    .prefixedWith(config.getStringProperty(CoreConfig.GRAPHITE_PREFIX))
                    .build(graphite);

            reporter.start(30l, TimeUnit.SECONDS);
        } else {
            reporter = null;
        }

        reporter2 = JmxReporter
                .forRegistry(registry)
                .convertDurationsTo(TimeUnit.SECONDS)
                .convertRatesTo(TimeUnit.MILLISECONDS)
                .build();
        reporter2.start();
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
