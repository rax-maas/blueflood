package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.PoolTimeoutException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Instrumentation implements InstrumentationMBean {
    private static QueryTimers timers = new QueryTimers();
    private static Meter writeErrMeter = Metrics.newMeter(Instrumentation.class, "Cassandra Write Errors", "Writes", TimeUnit.MINUTES);
    private static Meter readErrMeter = Metrics.newMeter(Instrumentation.class, "Cassandra Read Errors", "Reads", TimeUnit.MINUTES);
    private static final Logger log = LoggerFactory.getLogger(Instrumentation.class);
    // One-off meters
    private static Meter readStringsNotFound = Metrics.newMeter(Instrumentation.class, "String Metrics Not Found", "Operations", TimeUnit.MINUTES);
    private static Meter scanAllColumnFamiliesMeter = Metrics.newMeter(Instrumentation.class, "Scan all ColumnFamilies", "Scans", TimeUnit.MINUTES);
    private static Meter allPoolsExhaustedException = Metrics.newMeter(Instrumentation.class, "All Pools Exhausted", "Exceptions", TimeUnit.SECONDS);

    static {
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

    public static TimerContext getTimerContext(String query) {
        return timers.getTimerContext(query);
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
        private final Map<String, Timer> histograms = new HashMap<String, Timer>();

        public TimerContext getTimerContext(String query) {
            synchronized (query) {
                if (!histograms.containsKey(query)) {
                    histograms.put(query, Metrics.newTimer(Instrumentation.class, query, TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
                }
            }
            return histograms.get(query).time();
        }
    }

    public static void markStringsNotFound() {
        readStringsNotFound.mark();
    }

    public static void markScanAllColumnFamilies() {
        scanAllColumnFamiliesMeter.mark();
    }
}

interface InstrumentationMBean {}
