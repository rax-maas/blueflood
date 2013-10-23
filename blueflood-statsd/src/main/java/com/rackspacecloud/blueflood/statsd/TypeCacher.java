package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.statsd.containers.StatsCollection;
import com.rackspacecloud.blueflood.statsd.containers.Stat;
import com.rackspacecloud.blueflood.types.StatType;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TypeCacher extends AsyncFunctionWithThreadPool<StatsCollection, StatsCollection> {
    private static final Logger log = LoggerFactory.getLogger(TypeCacher.class);
    
    private final MetadataCache cache; 
    
    public TypeCacher(ThreadPoolExecutor executor, MetadataCache cache) {
        super(executor);
        this.cache = cache;
    }

    @Override
    public ListenableFuture<StatsCollection> apply(final StatsCollection input) throws Exception {
        
        return getThreadPool().submit(new Callable<StatsCollection>() {
            @Override
            public StatsCollection call() throws Exception {
                int cached = 0;
                
                for (StatType type : StatType.SIMPLE_TYPES) {
                    for (Stat stat : input.getStats(type)) {
                        if (stat.getLocator() != null) {
                            cache.put(stat.getLocator(), StatType.CACHE_KEY, type.toString());
                            cached += 1;
                        }
                    }
                }
                
                for (Locator locator : input.getTimerStats().keySet()) {
                    cache.put(locator, StatType.CACHE_KEY, StatType.TIMER.toString());
                    cached += 1;
                }
                return input;
            }
        });
    }
}
