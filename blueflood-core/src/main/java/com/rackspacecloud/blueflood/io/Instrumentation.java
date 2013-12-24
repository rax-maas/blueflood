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
    private static ReadTimers readTimers = new ReadTimers();
    private static WriteTimers writeTimers = new WriteTimers();
    private static final Meter writeErrMeter;
    private static final Meter readErrMeter;
    private static final Meter batchReadErrMeter;

    // One-off meters
    private static final Meter scanAllColumnFamiliesMeter;
    private static final Meter allPoolsExhaustedException;
    private static final Meter fullResMetricWritten;
    private static final Map<ColumnFamily, Meter> keyNotFoundInCFMap = new HashMap<ColumnFamily, Meter>();

    static {
        Class kls = Instrumentation.class;
        writeErrMeter = Metrics.meter(kls, "writes", "Cassandra Write Errors");
        readErrMeter = Metrics.meter(kls, "reads", "Cassandra Read Errors");
        batchReadErrMeter = Metrics.meter(kls, "reads", "Batch Cassandra Reads Errors");
        scanAllColumnFamiliesMeter = Metrics.meter(kls, "Scan all ColumnFamilies");
        allPoolsExhaustedException = Metrics.meter(kls, "All Pools Exhausted");
        fullResMetricWritten = Metrics.meter(kls, "Full Resolution Metrics Written");
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
        return readTimers.getTimerContext(queryCF, false);
    }

    public static Timer.Context getBatchReadTimerContext(ColumnFamily queryCF) {
        return readTimers.getTimerContext(queryCF, true);
    }

    public static Timer.Context getWriteTimerContext(ColumnFamily queryCF) {
        return writeTimers.getTimerContext(queryCF, false);
    }

    public static Timer.Context getBatchWriteTimerContext(ColumnFamily queryCF) {
        return writeTimers.getTimerContext(queryCF, true);
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

    public static void markBatchReadError(ConnectionException e) {
        batchReadErrMeter.mark();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
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

    private static class ReadTimers {
        private final Map<ColumnFamily, Timer> cfTimers = new HashMap<ColumnFamily, Timer>();
        private final Map<ColumnFamily, Timer> cfBatchTimers = new HashMap<ColumnFamily, Timer>();

        public Timer.Context getTimerContext(ColumnFamily queryCF, boolean batch) {
            final Map<ColumnFamily, Timer> map = (batch ? cfBatchTimers : cfTimers);
            synchronized (queryCF) {
                if (!map.containsKey(queryCF)) {
                    final String metricName = (batch ? MetricRegistry.name("batched-", queryCF.getName()) : queryCF.getName());
                    map.put(queryCF, Metrics.timer(Instrumentation.class, "reads", metricName));
                }
            }
            return map.get(queryCF).time();
        }
    }

    private static class WriteTimers {
        private final Map<ColumnFamily, Timer> cfTimers = new HashMap<ColumnFamily, Timer>();
        private final Map<ColumnFamily, Timer> cfBatchTimers = new HashMap<ColumnFamily, Timer>();

        public Timer.Context getTimerContext(ColumnFamily queryCF, boolean batch) {
            final Map<ColumnFamily, Timer> map = (batch ? cfBatchTimers : cfTimers);
            synchronized (queryCF) {
                if (!map.containsKey(queryCF)) {
                    final String metricName = (batch ? MetricRegistry.name("batched", queryCF.getName()) : queryCF.getName());
                    map.put(queryCF, Metrics.timer(Instrumentation.class, "writes", metricName));
                }
            }
            return map.get(queryCF).time();
        }
    }

    public static void markNotFound(ColumnFamily CF) {
        synchronized (CF) {
            Meter meter = keyNotFoundInCFMap.get(CF);
            if (meter == null) {
                meter = Metrics.meter(Instrumentation.class, "reads", "Not Found", CF.getName());
                keyNotFoundInCFMap.put(CF, meter);
            }
            meter.mark();
        }
    }

    public static void markScanAllColumnFamilies() {
        scanAllColumnFamiliesMeter.mark();
    }

    public static void markFullResMetricWritten() {
        fullResMetricWritten.mark();
    }
}

interface InstrumentationMBean {}
