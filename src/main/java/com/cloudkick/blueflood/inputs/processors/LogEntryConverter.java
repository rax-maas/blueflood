package com.cloudkick.blueflood.inputs.processors;

import com.cloudkick.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.inputs.handlers.ScribeHandler;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.MetricsCollection;
import com.cloudkick.fb303.Fb303Handler;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.TException;
import scribe.thrift.LogEntry;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class LogEntryConverter extends AsyncFunctionWithThreadPool<List<LogEntry>, MetricsCollection> {

    private static final long HOURS_24_IN_MILLIS = 1000 * 60 * 60 * 24;
    
    // todo: these metrics need to be attached to the right class.
    private final Timer deserializeTimer = Metrics.newTimer(ScribeHandler.class, "Thrift Deserialization", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Counter telescopeCount = Metrics.newCounter(ScribeHandler.class, "Telescopes Created");
    private final Counter nonTelescopeCount = Metrics.newCounter(ScribeHandler.class, "Non-telescope Count");
    private final Counter logEntryCount = Metrics.newCounter(ScribeHandler.class, "Log Entry Count");
    private final Histogram metricsPerBundle = Metrics.newHistogram(ScribeHandler.class, "Metrics Per Bundle", true);
    private final Meter corruptedTelescopes = Metrics.newMeter(ScribeHandler.class, "Corrupted Telescopes", "Rollups", TimeUnit.SECONDS);
    
    // todo: if the interface on ScheduleContext coulc be pared down, all this class needs is getCurrentTimeMillis().
    private final Ticker ticker;
    private final Fb303Handler scribeImpl;
    private final Counter bufferedMetrics;
    
    public LogEntryConverter(ListeningExecutorService threadPool, Ticker ticker, Fb303Handler scribeImpl, Counter bufferedMetrics) {
        super(threadPool);
        this.ticker = ticker;
        this.scribeImpl = scribeImpl;
        this.bufferedMetrics = bufferedMetrics;
    }
    
    @Override
    public ListenableFuture<MetricsCollection> apply(final List<LogEntry> input) throws Exception {
        return getThreadPool().submit(new Callable<MetricsCollection>() {
            public MetricsCollection call() throws Exception {
                return getTelescopesFromScribeMessages(input);
            }
        });
    }
    
    private MetricsCollection getTelescopesFromScribeMessages(List<LogEntry> messages) {
        String category = "";
        long timestamp = 0;
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
                getLogger().error("Failed to decode: {}" + message.message);
                skip = true;
            } catch (IllegalArgumentException ex){
                getLogger().error("Invalid telescope: ", ex);
                corruptedTelescopes.mark();
                skip = true;
            } catch (RuntimeException ex) {
                getLogger().error("Invalid telescope: ", ex);
                nonTelescopeCount.inc();
                skip = true;
            } finally {
                context.stop();
            }

            if (skip) {
                continue;
            }
            
            if (cmTelescope == null) {
                getLogger().error("Unexpected NULL cmTelescope");
                continue;
            } else if (cmTelescope.getTelescope() == null) {
                getLogger().error("Unexpected NULL telescope");
                continue;
            }

            if (cmTelescope.getTelescope().getTimestamp() - ticker.read() > HOURS_24_IN_MILLIS) {
                getLogger().warn("Dropping delayed telescope: " + cmTelescope.toString());
                continue;
            }

            final List<Metric> metrics = cmTelescope.toMetrics();
            if (metrics != null) {
                metricsCollection.add(metrics);
                timestamp = cmTelescope.getTelescope().getTimestamp();
            }

            category = message.category;
            telescopeCount.inc();
        }

        bufferedMetrics.inc(metricsCollection.size());
        metricsPerBundle.update(metricsCollection.size());
        scribeImpl.updateLastProcessedTime(category, timestamp);
        scribeImpl.updateMessageCounter(category, messages.size());

        return metricsCollection;
    }
}
