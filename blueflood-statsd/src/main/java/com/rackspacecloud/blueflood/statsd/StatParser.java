package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatsCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Stats sent to graphite from statsd come in this format:
 * {label} {value} {timtestamp}
 */
public class StatParser extends AsyncFunctionWithThreadPool<Collection<CharSequence>, StatsCollection> {
    private static Logger log = LoggerFactory.getLogger(StatParser.class);
    private static Meter invalidStatsMeter = Metrics.newMeter(StatParser.class, "Invalid Stats", "Ingestion", TimeUnit.MINUTES);
    private static Meter statParseErrors = Metrics.newMeter(StatParser.class, "Stat Parse Errors", "Ingestion", TimeUnit.MINUTES);
    
    public StatParser(ThreadPoolExecutor executor) {
        super(executor);
    }

    @Override
    public ListenableFuture<StatsCollection> apply(final Collection<CharSequence> input) throws Exception {
        return getThreadPool().submit(new Callable<StatsCollection>() {
            @Override
            public StatsCollection call() throws Exception {
                StatsCollection stats = new StatsCollection();
                for (CharSequence seq : input) {
                    try {
                        Stat stat = Conversions.asStat(seq);
                        if (stat.isValid()) {
                            stats.add(stat);
                        } else {
                            log.debug("Invalid stat line: {}", seq.toString());
                            invalidStatsMeter.mark();
                        }
                    } catch (Throwable nastyParseBug) {
                        log.error(nastyParseBug.getMessage(), nastyParseBug);
                        statParseErrors.mark();
                    }
                }
                return stats;
            }
        });
    }
}
