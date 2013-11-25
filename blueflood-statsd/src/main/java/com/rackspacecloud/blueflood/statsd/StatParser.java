package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.statsd.containers.Conversions;
import com.rackspacecloud.blueflood.statsd.containers.StatsCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stats sent to graphite from statsd come in this format:
 * {label} {value} {timtestamp}
 */
public class StatParser extends AsyncFunctionWithThreadPool<Collection<CharSequence>, StatsCollection> {
    private static Logger log = LoggerFactory.getLogger(StatParser.class);
    
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
                        }
                    } catch (Throwable nastyParseBug) {
                        log.error(nastyParseBug.getMessage(), nastyParseBug);
                    }
                }
                return stats;
            }
        });
    }
}
