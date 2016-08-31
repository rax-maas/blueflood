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
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class Instrumentation implements InstrumentationMBean {
    private static final Logger log = LoggerFactory.getLogger(Instrumentation.class);
    private static ReadTimers readTimers = new ReadTimers();
    private static WriteTimers writeTimers = new WriteTimers();
    private static final Meter writeErrMeter;
    private static final Meter readErrMeter;
    private static final Meter batchReadErrMeter;
    private static final Meter excessEnumWriteErrMeter;
    private static final Meter excessEnumReadErrMeter;

    // One-off meters
    private static final Meter scanAllColumnFamiliesMeter;
    private static final Meter allPoolsExhaustedException;
    private static final Meter fullResMetricWritten;
    private static final Meter fullResPreaggregatedMetricWritten;
    private static final Meter metricsWithShortDelayReceived;
    private static final Meter metricsWithLongDelayReceived;
    private static final Meter enumMetricWritten;

    static {
        Class kls = Instrumentation.class;
        excessEnumWriteErrMeter = Metrics.meter( kls, "writes", "Excess Enum Metrics Write Errors" );
        excessEnumReadErrMeter = Metrics.meter( kls, "reads", "Excess Enum Metrics Read Errors" );
        writeErrMeter = Metrics.meter(kls, "writes", "Cassandra Write Errors");
        readErrMeter = Metrics.meter(kls, "reads", "Cassandra Read Errors");
        batchReadErrMeter = Metrics.meter(kls, "reads", "Batch Cassandra Read Errors");
        scanAllColumnFamiliesMeter = Metrics.meter(kls, "Scan all ColumnFamilies");
        allPoolsExhaustedException = Metrics.meter(kls, "All Pools Exhausted");
        fullResMetricWritten = Metrics.meter(kls, "Full Resolution Metrics Written");
        fullResPreaggregatedMetricWritten = Metrics.meter(kls, "Full Resolution Preaggregated Metrics Written");
        enumMetricWritten = Metrics.meter( kls, "Enum Metrics Written" );
        metricsWithShortDelayReceived = Metrics.meter(kls, "Metrics with short delay received");
        metricsWithLongDelayReceived = Metrics.meter(kls, "Metrics with long delay received");

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

    public static Timer.Context getReadTimerContext(String queryCF) {
        return readTimers.getTimerContext(queryCF, false);
    }

    public static Timer.Context getBatchReadTimerContext(String queryCF) {
        return readTimers.getTimerContext(queryCF, true);
    }

    public static Timer.Context getWriteTimerContext(String queryCF) {
        return writeTimers.getTimerContext(queryCF, false);
    }

    public static Timer.Context getBatchWriteTimerContext(String queryCF) {
        return writeTimers.getTimerContext(queryCF, true);
    }

    // Most error tracking is done in InstrumentedConnectionPoolMonitor
    // However, some issues can't be properly tracked using that alone.
    // For example, there is no good way to differentiate (in the connectionpoolmonitor)
    // a PoolTimeoutException indicating that Astyanax has to look in another host-specific-pool
    // to find a connection from one indicating Astyanax already looked in every pool and is
    // going to bubble up the exception to our reader/writer
    public static void markReadError() {
        readErrMeter.mark();
    }

    public static void markBatchReadError(ConnectionException e) {
        batchReadErrMeter.mark();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
    }

    /**
     * Caller should explicitly call {@link com.rackspacecloud.blueflood.io.Instrumentation#markPoolExhausted()}
     * and {@link com.rackspacecloud.blueflood.io.Instrumentation#markReadError()}
     * @param e
     */
    @Deprecated
    public static void markReadError(ConnectionException e) {
        markReadError();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
    }

    public static void markPoolExhausted() {
        allPoolsExhaustedException.mark();
    }

    public static void markWriteError() {
        writeErrMeter.mark();
    }

    public static void markExcessEnumWriteError() { excessEnumWriteErrMeter.mark(); }

    public static void markExcessEnumReadError() { excessEnumReadErrMeter.mark(); }

    public static void markWriteError(ConnectionException e) {
        markWriteError();
        if (e instanceof PoolTimeoutException) {
            allPoolsExhaustedException.mark();
        }
    }

    private static class ReadTimers {
        public Timer.Context getTimerContext(String queryCF, boolean batch) {
            final String metricName = (batch ? MetricRegistry.name("batched-", queryCF) : queryCF);

            final Timer timer = Metrics.timer(Instrumentation.class, "reads", metricName);
            return timer.time();
        }
    }

    private static class WriteTimers {
        public Timer.Context getTimerContext(String queryCF, boolean batch) {
            final String metricName = (batch ? MetricRegistry.name("batched", queryCF) : queryCF);

            final Timer timer = Metrics.timer(Instrumentation.class, "writes", metricName);
            return timer.time();
        }
    }

    public static void markNotFound(String columnFamilyName) {
        final Meter meter = Metrics.meter(Instrumentation.class, "reads", "Not Found", columnFamilyName);
        meter.mark();

    }

    public static void markScanAllColumnFamilies() {
        scanAllColumnFamiliesMeter.mark();
    }

    public static void markFullResMetricWritten() {
        fullResMetricWritten.mark();
    }

    public static void markFullResPreaggregatedMetricWritten() {
        fullResPreaggregatedMetricWritten.mark();
    }

    public static void markEnumMetricWritten() {
        enumMetricWritten.mark();
    }

    public static void markMetricsWithShortDelayReceived() {
        metricsWithShortDelayReceived.mark();
    }

    public static void markMetricsWithLongDelayReceived() {
        metricsWithLongDelayReceived.mark();
    }
}