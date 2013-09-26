package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stats sent to graphite from statsd come in this format:
 * {label} {value} {timtestamp}
 */
public class StatParser extends AsyncFunctionWithThreadPool<Collection<CharSequence>, Collection<Stat>> {
    private static Logger log = LoggerFactory.getLogger(StatParser.class);
    
    public StatParser(ThreadPoolExecutor executor) {
        super(executor);
    }

    @Override
    public ListenableFuture<Collection<Stat>> apply(final Collection<CharSequence> input) throws Exception {
        return getThreadPool().submit(new Callable<Collection<Stat>>() {
            @Override
            public Collection<Stat> call() throws Exception {
                Collection<Stat> stats = new ArrayList<Stat>(input.size());
                for (CharSequence seq : input)
                    stats.add(Stat.fromLine(seq));
                return stats;
            }
        });
    }
}
