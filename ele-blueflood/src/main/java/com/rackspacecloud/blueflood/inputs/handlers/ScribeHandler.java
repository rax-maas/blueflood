package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.RackIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.cm.Util;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.util.JmxGauge;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

// todo: CM_SPECIFIC
// todo: We need to get rid of this. I think it's safe.
@Deprecated
public class ScribeHandler implements ScribeHandlerMBean, ScribeHandlerIface {
    private static final Logger log = LoggerFactory.getLogger(ScribeHandler.class);
    private static final long HOURS_24_IN_MILLIS = 1000 * 60 * 60 * 24;
    private static final TimeValue scribeTimeout = new TimeValue(4, TimeUnit.SECONDS);
    
    // Timing
    private final Timer deserializeTimer = Metrics.newTimer(ScribeHandler.class, "Thrift Deserialization", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer logCallTimer = Metrics.newTimer(ScribeHandler.class, "Scribe Log Call Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer writeDurationTimer = Metrics.newTimer(ScribeHandler.class, "Write Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    // counters.
    private final Counter telescopeCount = Metrics.newCounter(ScribeHandler.class, "Telescopes Created");
    private final Counter nonTelescopeCount = Metrics.newCounter(ScribeHandler.class, "Non-telescope Count");
    private final Counter logCallCount = Metrics.newCounter(ScribeHandler.class, "Log Call Count");
    private final Counter logEntryCount = Metrics.newCounter(ScribeHandler.class, "Log Entry Count");
    private final Counter bufferedMetrics = Metrics.newCounter(ScribeHandler.class, "Buffered Metrics");
    private final Histogram metricsPerBundle = Metrics.newHistogram(ScribeHandler.class, "Metrics Per Bundle", true);
    private final Meter corruptedTelescopes = Metrics.newMeter(ScribeHandler.class,
            "Corrupted Telescopes", "Rollups", TimeUnit.MINUTES);
    private final Meter exceededScribeProcessingTime = Metrics.newMeter(ScribeHandler.class,
            "Write Duration Exceeded Timeout", "Rollups", TimeUnit.MINUTES);
    private Gauge inflightWriteGauge;
    private Gauge queuedWriteGauge;
    private Gauge writeBatchSizeGauge;
    private Gauge writeConcurrencyGauge;

    private final ThreadPoolExecutor writeExecutors;
    private static int WRITE_THREADS = 50;
    
    private ScheduleContext context;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer;
    private Integer scribeBatchId;
    
    private AbstractScribeHandler scribeImpl;

    public ScribeHandler(ScheduleContext context) {
        this.scribeImpl = new AbstractScribeHandler() {
            @Override
            public ResultCode Log(List<LogEntry> messages) throws TException {
                return ScribeHandler.this.Log(messages);
            }
        };
        
        this.context = context;
        
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String objName = String.format("com.rackspacecloud.blueflood.inputs.handlers:type=%s", getClass().getSimpleName());
            mbs.registerMBean(this, new ObjectName(objName));
            inflightWriteGauge = Metrics.newGauge(ScribeHandler.class, "In Flight Writes",
                    new JmxGauge(objName, "InFlightWriteCount"));
            queuedWriteGauge = Metrics.newGauge(ScribeHandler.class, "Queued Writes",
                    new JmxGauge(objName, "QueuedWriteCount"));
            writeBatchSizeGauge = Metrics.newGauge(ScribeHandler.class, "Write Batch Size",
                    new JmxGauge(objName, "WriteBatchSize"));
            writeConcurrencyGauge = Metrics.newGauge(ScribeHandler.class, "Write Concurrency",
                    new JmxGauge(objName, "WriteConcurrency"));
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + getClass().getSimpleName(), exc);
        }
        
        // unbounded work queue.
        final BlockingQueue<Runnable> writeQueue = new LinkedBlockingQueue<Runnable>();
        WRITE_THREADS = Configuration.getIntegerProperty("MAX_SCRIBE_WRITE_THREADS");
        writeExecutors = new ThreadPoolExecutor( 
            WRITE_THREADS,
            WRITE_THREADS,
            30, TimeUnit.SECONDS,
            writeQueue,
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy()
        );
        metricMetadataAnalyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

        this.scribeBatchId = 0;
    }
    
    public scribe.Iface getScribe() {
        return this.scribeImpl;
    }
    
    public ResultCode Log(final List<LogEntry> messages) throws TException {
        logCallCount.inc();
        ++scribeBatchId;

        TimerContext lctContext = logCallTimer.time();
        final MetricsCollection metricsCollection = getMetricsCollectionFromScribeMessages(messages);
        processMetricTypeAndUnit(metricsCollection);

        // we have two latches that count down as the batches are executed.  Here's why:
        // we wait on the short latch as long as the distribution scribe can tolerate (usually 5s, so we wait 4s). 
        // if the short latch hasn't counted down by then, we assume things are going OK and return OK.
        // the long latch is waited on for a much longer time (2m). if it times out before counting down, we
        // log an error even though we've already returned OK.
        ResultCode result;
        final AtomicBoolean persistedScribeBatch = new AtomicBoolean(true);
        final List<List<Metric>> batches = MetricsCollection.getMetricsAsBatches(metricsCollection, WRITE_THREADS);
        final CountDownLatch shortLatch = new CountDownLatch(batches.size());
        persistMetrics(scribeBatchId, batches, shortLatch, persistedScribeBatch);

        // wait for 4 seconds (scribe will retry the batch after 5 seconds if we do not respond fast enough).
        boolean sentInTime = true;
        try {
            sentInTime = shortLatch.await(scribeTimeout.getValue(), scribeTimeout.getUnit());
        } catch (InterruptedException ex) {
            log.error(ex.getMessage(), ex);
            persistedScribeBatch.set(false);
        }
        
        if (!persistedScribeBatch.get()) {
            log.error("Errors prevented persisting all telescopes");
            result = ResultCode.TRY_LATER;
        } else {
            if (sentInTime)
                log.debug("Processed all of {} messages", messages.size());
            else
                log.debug("Processed a subset of {} messages", messages.size());
            result = ResultCode.OK;
        }

        if (!sentInTime) {
            log.warn("Did not process " + shortLatch.getCount() + " batches of metrics within "
                    + scribeTimeout.toString() + " for batch " + scribeBatchId);
            result = ResultCode.OK; // because we don't want things to get backed up.
        }

        lctContext.stop();

        if (scribeBatchId >= Integer.MAX_VALUE) {
            scribeBatchId = 0;
        }

        return result;
    }

    private MetricsCollection getMetricsCollectionFromScribeMessages(List<LogEntry> messages) {
        // convert LogEntry to Telescope.

        final MetricsCollection metricsCollection = new MetricsCollection();
        for (LogEntry message : messages) {
            logEntryCount.inc();

            TimerContext context = deserializeTimer.time();
            CloudMonitoringTelescope cmTelescope = null;
            boolean skip = false;
            try {
                cmTelescope = new CloudMonitoringTelescope(message);
            } catch (TException ex) {
                // problem with the thrift message.
                log.error("Failed to decode: {}" + message.message);
                skip = true;
            } catch (IllegalArgumentException ex) {
                log.error("Invalid telescope: ", ex);
                corruptedTelescopes.mark();
                skip = true;
            } catch (RuntimeException ex) {
                log.error("Invalid telescope: ", ex);
                nonTelescopeCount.inc();
                skip = true;
            } finally {
                context.stop();
            }

            if (skip) {
                continue;
            }

            if (cmTelescope.getTelescope().getTimestamp() - this.context.getCurrentTimeMillis() > HOURS_24_IN_MILLIS) {
                log.warn("Dropping delayed telescope: " + cmTelescope.toString());
                continue;
            }

            final List<Metric> metrics = cmTelescope.toMetrics();
            if (metrics != null) {
                updateContext(metrics);
                metricsCollection.add(metrics);
            }

            telescopeCount.inc();
        }

        bufferedMetrics.inc(metricsCollection.size());
        metricsPerBundle.update(metricsCollection.size());

        return metricsCollection;
    }

    private void processMetricTypeAndUnit(final MetricsCollection metricsCollection) {
        // this will end up doing some reads and writes.
        // todo: a side-effect of analyzing telescopes outside of the writer is that Locators get created and then
        // quickly discarded.  That is wasteful.  A better approach would use several stages:
        // state 1: telescope collection is transformed into (locator, value) collection.
        // state 2: (locator, value) collection is analyzed.
        // state 3: (locator, value) collection is ingested.
        writeExecutors.execute(new Runnable() {
            public void run() {
                Collection<IncomingMetricException> problems =
                        metricMetadataAnalyzer.scanMetrics(metricsCollection.getMetrics());
                for (IncomingMetricException problem : problems)
                    // TODO: this is where a system annotation should be raised.
                    log.warn(problem.getMessage());
            }
        });
    }

    private void persistMetrics(final Integer batchId,
                                final List<List<Metric>> metricsBatches,
                                final CountDownLatch shortLatch,
                                final AtomicBoolean successfullyPersisted) {
        // execute the batches in a threadpool. keep track of how long it takes to execute all the batches.
        final AtomicBoolean writeTimedOut = new AtomicBoolean(false);
        final long writeStartTime = System.currentTimeMillis();
        final TimerContext actualWriteCtx = writeDurationTimer.time();

        for (final List<Metric> batch : metricsBatches) {
            writeExecutors.execute(new Runnable() {
                public void run() {
                    try {
                        AstyanaxWriter.getInstance().insertFull(batch);
                        // todo: CM_SPECIFIC
                        RackIO.getInstance().insertDiscovery(batch);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                        successfullyPersisted.set(false);
                    } finally {
                        shortLatch.countDown();
                        bufferedMetrics.dec(batch.size());

                        if (System.currentTimeMillis() - writeStartTime > scribeTimeout.toMillis()) {
                            writeTimedOut.set(true);
                        }
                        done();
                    }
                }

                private void done() {
                    if (shortLatch.getCount() == 0) {
                        actualWriteCtx.stop();

                        if (writeTimedOut.get()) {
                            exceededScribeProcessingTime.mark();
                            log.error("Exceeded scribe timeout " + scribeTimeout.toString() + " before persisting " +
                                    "all metrics for scribe batch " + batchId);
                        }

                        if (!successfullyPersisted.get()) {
                            log.warn("Did not persist all metrics successfully for scribe batch " + batchId);
                        }
                    }
                }
            });
        }
    }

    // XXX: In our current design, the context is updated about dirty slots from here which is never used in production.
    // Also, updating context from input handler is a tightly coupled design.
    private void updateContext(List<Metric> metrics) {
        if (metrics != null) {
            for (Metric metric : metrics) {
                context.update(metric.getCollectionTime(), Util.computeShard(metric.getLocator().toString()));
            }
        }
    }

    //
    // JMX stuff
    //
    public synchronized int getQueuedWriteCount() { return writeExecutors.getQueue().size(); }
    public synchronized int getInFlightWriteCount() { return writeExecutors.getActiveCount(); }
    
    public synchronized int getWriteConcurrency() {
        return writeExecutors.getMaximumPoolSize();
    }

    public synchronized void setWriteConcurrency(int i) {
        writeExecutors.setCorePoolSize(i);
        writeExecutors.setMaximumPoolSize(i);
    }
}
