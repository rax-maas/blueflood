package com.cloudkick.blueflood.inputs.handlers;

import com.cloudkick.blueflood.cache.MetadataCache;
import com.cloudkick.blueflood.cache.TtlCache;
import com.cloudkick.blueflood.concurrent.AsyncChain;
import com.cloudkick.blueflood.concurrent.ThreadPoolBuilder;
import com.cloudkick.blueflood.inputs.processors.BatchSplitter;
import com.cloudkick.blueflood.inputs.processors.BatchWriter;
import com.cloudkick.blueflood.inputs.processors.LogEntryConverter;
import com.cloudkick.blueflood.inputs.processors.TtlAffixer;
import com.cloudkick.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.service.Configuration;
import com.cloudkick.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.cloudkick.blueflood.service.ScheduleContext;
import com.cloudkick.blueflood.internal.Account;
import com.cloudkick.blueflood.internal.InternalAPIFactory;
import com.cloudkick.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.util.JmxGauge;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


public class AlternateScribeHandler implements ScribeHandlerMBean, ScribeHandlerIface {
    private static final Logger log = LoggerFactory.getLogger(ScribeHandler.class);
    private static final TimeValue scribeTimeout = new TimeValue(4, TimeUnit.SECONDS);
    private static int WRITE_THREADS = 50; // metrics will be batched into this many partitions.
    
    private static TtlCache FULLRES_TTL_CACHE = new TtlCache(
            "Full Res TTL Cache",
            new TimeValue(120, TimeUnit.HOURS),
            Integer.parseInt(Configuration.getStringProperty("MAX_ROLLUP_THREADS")),
            InternalAPIFactory.createDefaultTTLProvider()) {
        // we only care about caching full res values.
        @Override
        protected Map<String, TimeValue> buildTtlMap(Account acct) {
            Map<String, TimeValue> map = new HashMap<String, TimeValue>();
            map.put("full", acct.getMetricTtl("full"));
            return map;
        }
    };
    
    // Timing
    private final Timer logCallTimer = Metrics.newTimer(ScribeHandler.class, "Scribe Log Call Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    // counters.
    private final Counter logCallCount = Metrics.newCounter(ScribeHandler.class, "Log Call Count");
    
    // NOTE: this is shared between a LogEntryConverter and a BatchWriter (former increments, latter decrements).
    private final Counter bufferedMetrics = Metrics.newCounter(ScribeHandler.class, "Buffered Metrics");
    
    // todo: these gauges do not seem to be used anywhere.
    private Gauge inflightWriteGauge;
    private Gauge queuedWriteGauge;
    private Gauge writeConcurrencyGauge;

    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer = new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;
    private AbstractScribeHandler scribeImpl;
    
    private final AsyncChain<List<LogEntry>, List<Boolean>> telescopeProcessor;

    public AlternateScribeHandler(ScheduleContext context) {
        this.context = context;

        this.scribeImpl = new AbstractScribeHandler() {
            @Override
            public ResultCode Log(List<LogEntry> messages) throws TException {
                return AlternateScribeHandler.this.Log(messages);
            }
        };
        
        // set up the processor
        telescopeProcessor = new AsyncChain<List<LogEntry>, List<Boolean>>()
            .withFunction(new LogEntryConverter(
                new ThreadPoolBuilder().withName("Telescope parsing").build(),
                context.asMillisecondsSinceEpochTicker(),
                bufferedMetrics)
                .withLogger(log))
            .withFunction(new TypeAndUnitProcessor(
                new ThreadPoolBuilder().withName("Metric type and unit processing").build(),
                metricMetadataAnalyzer)
                .withLogger(log))
            .withFunction(new TtlAffixer(
                new ThreadPoolBuilder().withName("TTL fixing").build(),
                FULLRES_TTL_CACHE)
                .withLogger(log))
            .withFunction(new BatchSplitter(
                new ThreadPoolBuilder().withName("Metric batching").build(),
                    WRITE_THREADS)
                .withLogger(log))
            .withFunction(new BatchWriter(
                new ThreadPoolBuilder()
                    .withName("Metric Batch Writing")
                    .withCorePoolSize(WRITE_THREADS)
                    .withMaxPoolSize(WRITE_THREADS)
                    .withUnboundedQueue()
                    .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                    .build(),
                AstyanaxWriter.getInstance(),
                scribeTimeout,
                bufferedMetrics,
                context)
                .withLogger(log));
        
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String objName = String.format("com.cloudkick.blueflood.inputs.handlers:type=%s", ScribeHandler.class.getSimpleName());
            StandardMBean mbean = new StandardMBean(this, ScribeHandlerMBean.class);
            mbs.registerMBean(mbean, new ObjectName(objName));
            inflightWriteGauge = Metrics.newGauge(ScribeHandler.class, "In Flight Writes",
                    new JmxGauge(objName, "InFlightWriteCount"));
            queuedWriteGauge = Metrics.newGauge(ScribeHandler.class, "Queued Writes",
                    new JmxGauge(objName, "QueuedWriteCount"));
            writeConcurrencyGauge = Metrics.newGauge(ScribeHandler.class, "Write Concurrency",
                    new JmxGauge(objName, "WriteConcurrency"));
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + getClass().getSimpleName(), exc);
        }
    }
    
    public scribe.Iface getScribe() {
        return this.scribeImpl;
    }
    
    public ResultCode Log(List<LogEntry> messages) throws TException {
        
        // housekeeping.
        logCallCount.inc();

        TimerContext lctContext = logCallTimer.time();
        final AtomicBoolean persistedScribeBatch = new AtomicBoolean(true);
        
        ResultCode result = ResultCode.OK;
        
        boolean sentInTime = true;
        try {
            List<Boolean> batchResults = telescopeProcessor.apply(messages).get(scribeTimeout.getValue(), scribeTimeout.getUnit());
            for (Boolean batchResult : batchResults) {
                if (batchResult == false) {
                    return ResultCode.TRY_LATER;
                }
            }
            return ResultCode.OK;
        } catch (TimeoutException ex) {
            sentInTime = false; // stuff might still be happening though.
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            persistedScribeBatch.set(false);
        } finally {
            lctContext.stop();
        }
        
        if (!persistedScribeBatch.get()) {
            log.error("Errors prevented persisting all telescopes");
            result = ResultCode.TRY_LATER;
        } else {
            if (sentInTime)
                log.debug("Processed all of {} messages", messages.size());
            else
                log.debug("Processed a subset of {} messages", messages.size());
        }
        
        if (!sentInTime) {
            log.warn("Did not process some batches of metrics within "
                    + scribeTimeout.toString());
            result = ResultCode.OK; // because we don't want things to get backed up.
        }
        
        return result;
    }
    
    //
    // JMX Stuff
    //


    // todo: fix queue/concurrency methods.
    public synchronized int getQueuedWriteCount() { return 0; }
    public synchronized int getInFlightWriteCount() { return 0; }
    public synchronized int getWriteConcurrency() { return 0; }
    public synchronized void setWriteConcurrency(int i) { }
}