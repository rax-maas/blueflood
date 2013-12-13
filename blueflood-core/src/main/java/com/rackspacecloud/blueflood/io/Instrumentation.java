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

package com.rackspacecloud.blueflood.io;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class Instrumentation implements InstrumentationMBean {
    private static final Logger log = LoggerFactory.getLogger(Instrumentation.class);
    private static QueryTimers queryTimers = new QueryTimers();
    private static UpdateTimers updateTimers = new UpdateTimers();
    private static final Meter writeErrMeter;
    private static final Meter readErrMeter;

    // One-off meters
    private static final Meter readStringsNotFound;
    private static final Meter scanAllColumnFamiliesMeter;
    private static final Meter allPoolsExhaustedException;
    private static final Meter fullResMetricWritten;
    private static final Map<ColumnFamily, Meter> keyNotFoundInCFMap = new HashMap<ColumnFamily, Meter>();

    static {
        MetricRegistry reg = Metrics.getRegistry();
        Class kls = Instrumentation.class;
        writeErrMeter = reg.meter(MetricRegistry.name(kls, "Cassandra Write Errors"));
        readErrMeter = reg.meter(MetricRegistry.name(kls, "Cassandra Read Errors"));
        readStringsNotFound = reg.meter(MetricRegistry.name(kls, "String Metrics Not Found"));
        scanAllColumnFamiliesMeter = reg.meter(MetricRegistry.name(kls, "Scan all ColumnFamilies"));
        allPoolsExhaustedException = reg.meter(MetricRegistry.name(kls, "All Pools Exhausted"));
        fullResMetricWritten = reg.meter(MetricRegistry.name(kls, "Full Resolution Metrics Written"));
            try {
                final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                final String name = String.format("com.rackspacecloud.blueflood.io:type=%s", Instrumentation.class.getSimpleName());
                final ObjectName nameObj = new ObjectName(name);
                mbs.registerMBean(new Instrumentation() { }, nameObj);
            } catch (Exception exc) {
                log.error("Unable to register mbean for " + Instrumentation.class.getSimpleName(), exc);
            }
    }

    private Instrumentation() {/* Used for JMX exposure */}

    public static Timer.Context getReadTimerContext(ColumnFamily queryCF) {
        return queryTimers.getTimerContext(queryCF);
    }

    public static Timer.Context getReadTimerContext(String query) {
        return queryTimers.getTimerContext(query);
    }

    public static Timer.Context getWriteTimerContext(ColumnFamily queryCF) {
        return updateTimers.getTimerContext(queryCF);
    }

    public static Timer.Context getWriteTimerContext(String query) {
        return updateTimers.getTimerContext(query);
    }

    // Most error tracking is done in InstrumentedConnectionPoolMonitor
    // However, some issues can't be properly tracked using that alone.
    // For example, there is no good way to differentiate (in the connectionpoolmonitor)
    // a PoolTimeoutException indicating that Astyanax has to look in another host-specific-pool
    // to find a connection from one indicating Astyanax already looked in every pool and is
    // going to bubble up the exception to our reader/writer
    private static void markReadError() {
        readErrMeter.mark();
    }

    public static void markReadError(ConnectionException e) {
        markReadError();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
    }

    private static void markWriteError() {
        writeErrMeter.mark();
    }

    public static void markWriteError(ConnectionException e) {
        markWriteError();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
    }

    private static class QueryTimers {
        private final Map<ColumnFamily, Timer> cfhistograms = new HashMap<ColumnFamily, Timer>();
        private final Map<String, Timer> histograms = new HashMap<String, Timer>();

        private final MetricRegistry registry = Metrics.getRegistry();

        public Timer.Context getTimerContext(ColumnFamily queryCF) {
            synchronized (queryCF) {
                if (!cfhistograms.containsKey(queryCF)) {
                    final String metricName = "Read From " + queryCF.getName();
                    cfhistograms.put(queryCF, registry.timer(registry.name(Instrumentation.class, metricName)));
                }
            }
            return histograms.get(queryCF).time();
        }

        public Timer.Context getTimerContext(String query) {
            synchronized (histograms) {
                if (!histograms.containsKey(query)) {
                    histograms.put(query, registry.timer(registry.name(Instrumentation.class, query)));
                }
            }
            return histograms.get(query).time();
        }
    }

    private static class UpdateTimers {
        private final Map<ColumnFamily, Timer> cfhistograms = new HashMap<ColumnFamily, Timer>();
        private final Map<String, Timer> histograms = new HashMap<String, Timer>();

        private final MetricRegistry registry = Metrics.getRegistry();

        public Timer.Context getTimerContext(ColumnFamily queryCF) {
            synchronized (queryCF) {
                if (!cfhistograms.containsKey(queryCF)) {
                    final String metricName = "Write To  " + queryCF.getName();
                    cfhistograms.put(queryCF, registry.timer(registry.name(Instrumentation.class, metricName)));
                }
            }
            return cfhistograms.get(queryCF).time();
        }

        public Timer.Context getTimerContext(String query) {
            synchronized (query) {
                if (!histograms.containsKey(query)) {
                    histograms.put(query, registry.timer(registry.name(Instrumentation.class, query)));
                }
            }
            return histograms.get(query).time();
        }
    }

    public static void markNotFound(ColumnFamily CF) {
        synchronized (keyNotFoundInCFMap) {
            Meter meter = keyNotFoundInCFMap.get(CF);
            if (meter == null) {
                meter = Metrics.getRegistry().meter(MetricRegistry.name(Instrumentation.class,
                        "Key Not Found in " + CF.getName()));
                keyNotFoundInCFMap.put(CF,meter);
            }
            meter.mark();
        }
    }

    public static void markStringsNotFound() {
        readStringsNotFound.mark();
    }

    public static void markScanAllColumnFamilies() {
        scanAllColumnFamiliesMeter.mark();
    }

    public static void markFullResMetricWritten() {
        fullResMetricWritten.mark();
    }
}

interface InstrumentationMBean {}
