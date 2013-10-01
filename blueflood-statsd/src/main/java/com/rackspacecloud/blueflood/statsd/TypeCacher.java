package com.rackspacecloud.blueflood.statsd;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class TypeCacher extends AsyncFunctionWithThreadPool<Collection<Stat>, Collection<Stat>> {
    private static final Logger log = LoggerFactory.getLogger(TypeCacher.class);
    
    private final MetadataCache cache; 
    
    public TypeCacher(ThreadPoolExecutor executor, MetadataCache cache) {
        super(executor);
        this.cache = cache;
    }

    @Override
    public ListenableFuture<Collection<Stat>> apply(final Collection<Stat> input) throws Exception {
        
        // this one is asyncrhonous.
        getThreadPool().submit(new Callable<Collection<Stat>>() {
            @Override
            public Collection<Stat> call() throws Exception {
                for (Stat stat : input) {
                    if (stat.getLocator() != null) {
                        cache.put(stat.getLocator(), StatType.CACHE_KEY, stat.getType().toString());
                    }
                }
                return input;
            }
        });
        
        return new NoOpFuture<Collection<Stat>>(input);
    }
}
