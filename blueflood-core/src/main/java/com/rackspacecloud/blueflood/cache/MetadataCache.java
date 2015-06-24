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

package com.rackspacecloud.blueflood.cache;


import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.AstyanaxMetadataIO;
import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.*;
import java.util.concurrent.*;

public class MetadataCache extends AbstractJmxCache implements MetadataCacheMBean {
    // todo: give each cache a name.

    private final com.google.common.cache.LoadingCache<CacheKey, String> cache;
    private static final String NULL = "null".intern();
    private static final Logger log = LoggerFactory.getLogger(MetadataCache.class);
    private static final TimeValue defaultExpiration = new TimeValue(Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_RETENTION_IN_MINUTES), TimeUnit.MINUTES);
    private static final int defaultConcurrency = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_MAX_CONCURRENCY);
    private final Boolean batchedReads;
    private final Boolean batchedWrites;

    // Specific to batched meta reads

    private static final Integer batchedReadsThreshold = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_READS_THRESHOLD);
    private static final Integer batchedReadsTimerConfig = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_READS_TIMER_MS);
    private static final TimeValue batchedReadsInterval = new TimeValue(batchedReadsTimerConfig, TimeUnit.MILLISECONDS);
    private static final Integer batchedReadsPipelineLimit = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_READS_PIPELINE_DEPTH);

    private final java.util.Timer batchedReadsTimer = new java.util.Timer("MetadataBatchedReads");
    private final ThreadPoolExecutor readThreadPoolExecutor;
    private final Set<Locator> outstandingMetaReads;
    private final Queue<Locator> metaReads; // Guarantees FIFO reads
    private static final Timer batchedReadsTimerMetric = Metrics.timer(MetadataCache.class, "Metadata batched reads timer");

    // Specific to batched meta writes

    private static final Integer batchedWritesThreshold = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_WRITES_THRESHOLD);
    private static final Integer batchedWritesTimerConfig = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_WRITES_TIMER_MS);
    private static final TimeValue batchedWritesInterval = new TimeValue(batchedWritesTimerConfig, TimeUnit.MILLISECONDS);
    private static final Integer batchedWritesPipelineLimit = Configuration.getInstance().getIntegerProperty(
            CoreConfig.META_CACHE_BATCHED_WRITES_PIPELINE_DEPTH);

    private final java.util.Timer batchedWritesTimer = new java.util.Timer("MetadataBatchedWrites");
    private final ThreadPoolExecutor writeThreadPoolExecutor;
    private final Set<CacheKey> outstandingMetaWrites;
    private final Queue<CacheKey> metaWrites; // Guarantees FIFO writes
    private static final Timer batchedWritesTimerMetric = Metrics.timer(MetadataCache.class, "Metadata batched writes timer");

    private static final MetadataCache INSTANCE = new MetadataCache(defaultExpiration, defaultConcurrency);
    private MetadataIO io = new AstyanaxMetadataIO();
    private static Timer cacheSaveTimer = Metrics.timer(MetadataCache.class, "Persistence Save");
    private static Timer cacheLoadTimer = Metrics.timer(MetadataCache.class, "Persistence Load");
    private static final Meter updatedMetricMeter = Metrics.meter(MetadataCache.class, "Received updated metric");
    private static final Histogram totalMetadataSize = Metrics.histogram(MetadataCache.class, "Metadata row size");
    private static final Timer cacheGetTimer = Metrics.timer(MetadataCache.class, "Metadata get timer");
    private static final Timer cachePutTimer = Metrics.timer(MetadataCache.class, "Metadata put timer");
    private final Gauge cacheSizeGauge = new Gauge<Long>() {
        @Override
        public Long getValue() {
            return cache.size();
        }
    };

    private MetadataCache(TimeValue expiration, int concurrency) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format(MetadataCache.class.getPackage().getName() + ":type=%s,name=Stats", MetadataCache.class.getSimpleName());
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            instantiateYammerMetrics(MetadataCache.class, "metadata", nameObj);
        } catch (InstanceAlreadyExistsException doNotCare) {
            log.debug(doNotCare.getMessage());
        } catch (Exception ex) {
            log.error("Unable to register mbean for " + getClass().getName(), ex);
        }

        CacheLoader<CacheKey, String> loader = new CacheLoader<CacheKey, String>() {
            @Override
            public String load(CacheKey key) throws Exception {
                return MetadataCache.this.databaseLoad(key.locator, key.keyString);
            }
        };
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiration.getValue(), expiration.getUnit())
                .concurrencyLevel(concurrency)
                .recordStats()
                .build(loader);
        try {
            Metrics.getRegistry().register(MetricRegistry.name(MetadataCache.class, "Cache Size"), this.cacheSizeGauge);
        } catch (Exception e) {
            // pass
        }
        this.outstandingMetaReads = new ConcurrentSkipListSet<Locator>();
        this.metaReads = new ConcurrentLinkedQueue<Locator>();
        this.readThreadPoolExecutor = new ThreadPoolBuilder().withCorePoolSize(batchedReadsPipelineLimit)
                .withMaxPoolSize(batchedReadsPipelineLimit)
                .withBoundedQueue(Configuration.getInstance()
                        .getIntegerProperty(CoreConfig.META_CACHE_BATCHED_READS_QUEUE_SIZE))
                .withName("MetaBatchedReadsThreadPool").build();

        this.batchedReads = Configuration.getInstance().getBooleanProperty(
                CoreConfig.META_CACHE_BATCHED_READS);
        this.batchedWrites = Configuration.getInstance().getBooleanProperty(
                CoreConfig.META_CACHE_BATCHED_WRITES);
        if (batchedReads) {
            this.batchedReadsTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    fetchMeta(true);
                }
            }, 0, this.batchedReadsInterval.toMillis());
        }
        this.outstandingMetaWrites = new ConcurrentSkipListSet<CacheKey>();
        this.writeThreadPoolExecutor = new ThreadPoolBuilder().withCorePoolSize(batchedWritesPipelineLimit)
                .withMaxPoolSize(batchedWritesPipelineLimit)
                .withBoundedQueue(Configuration.getInstance()
                        .getIntegerProperty(CoreConfig.META_CACHE_BATCHED_WRITES_QUEUE_SIZE))
                .withName("MetaBatchedWritesThreadPool").build();
        this.metaWrites = new ConcurrentLinkedQueue<CacheKey>();

        if (batchedWrites) {
            this.batchedWritesTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    flushMeta(true);
                }
            }, 0, this.batchedWritesInterval.toMillis());
        }
    }
    
    public void setIO(MetadataIO io) {
        this.io = io;
        cache.invalidateAll();
    }

    public static MetadataCache getInstance() {
        return INSTANCE;
    }

    public static MetadataCache createLoadingCacheInstance() {
        return new MetadataCache(defaultExpiration, defaultConcurrency);
    }

    public static MetadataCache createLoadingCacheInstance(TimeValue expiration, int concurrency) {
        return new MetadataCache(expiration, concurrency);
    }
    
    public void save(DataOutputStream out) throws IOException {
        
        Timer.Context ctx = cacheSaveTimer.time();
        try {
        // convert to a table. this avoids us writing out the locator over and over.
            Map<CacheKey, String> map = new HashMap<CacheKey, String>(cache.asMap());
            Table<Locator, String, String> table = HashBasedTable.create();
            for (Map.Entry<CacheKey, String> entry : map.entrySet()) {
                table.put(entry.getKey().locator, entry.getKey().keyString, entry.getValue());
            }
            
            Set<Locator> rowKeys = table.rowKeySet();
            out.writeInt(rowKeys.size());
            
            for (Locator locator : rowKeys) {
                out.writeUTF(locator.toString());
                
                // how many key/value pairs are there?
                Map<String, String> pairs = table.row(locator);
                out.writeInt(pairs.size());
                for (Map.Entry<String, String> entry : pairs.entrySet()) {
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }
            }
        } finally {
            ctx.stop();
        }
    }
    
    public void load(DataInputStream in) throws IOException {
        Timer.Context ctx = cacheLoadTimer.time();
        try {
            int numLocators = in.readInt();
            for (int locIndex = 0; locIndex < numLocators; locIndex++) {
                Locator locator = Locator.createLocatorFromDbKey(in.readUTF());
                int numPairs = in.readInt();
                for (int pairIndex = 0; pairIndex < numPairs; pairIndex++) {
                    cache.put(new CacheKey(locator, in.readUTF()), in.readUTF());
                }
            }
        } finally {
            ctx.stop();
        }
    }

    public boolean containsKey(Locator locator, String key) {
        return cache.getIfPresent(new CacheKey(locator, key)) != null;
    }

    public String get(Locator locator, String key) throws CacheException {
        if (!batchedReads) {
            return getImmediately(locator, key);
        }

        String val = cache.getIfPresent(new CacheKey(locator, key));

        if (val == null) {
            databaseLazyLoad(locator); // loads all meta for the locator (optimized to prevent duplicate reads)
        }

        return val;
    }

    public String getImmediately(Locator locator, String key) throws CacheException {
        Timer.Context cacheGetTimerContext = cacheGetTimer.time();
        try {
            CacheKey cacheKey = new CacheKey(locator, key);
            String result = cache.get(cacheKey);
            if (result.equals(NULL)) {
                return null;
            } else {
                return result;
            }
        } catch (ExecutionException ex) {
            throw new CacheException(ex);
        } finally {
            cacheGetTimerContext.stop();
        }
    }

    public <T> T get(Locator locator, String key, Class<T> type) throws CacheException {
        try {
            String val = get(locator, key);
            if (val == null) {
                return null;
            }
            return (T) val;
        } catch (ClassCastException ex) {
            throw new CacheException(ex);
        }
    }

    // todo: synchronization?
    // returns true if updated.
    public boolean put(Locator locator, String key, String value) throws CacheException {
        if (value == null) return false;

        Timer.Context cachePutTimerContext = MetadataCache.cachePutTimer.time();
        boolean dbWrite = false;
        try {
            CacheKey cacheKey = new CacheKey(locator, key);
            String oldValue = cache.getIfPresent(cacheKey);
            // don't care if oldValue == EMPTY.
            // always put new value in the cache. it keeps reads from happening.
            cache.put(cacheKey, value);
            if (oldValue == null || !oldValue.equals(value)) {
                dbWrite = true;
            }

            if (dbWrite) {
                updatedMetricMeter.mark();
                if (!batchedWrites) {
                    databasePut(locator, key, value);
                } else {
                    databaseLazyWrite(locator, key);
                }
            }

            return dbWrite;
        } finally {
            cachePutTimerContext.stop();
        }
    }

    public void invalidate(Locator locator, String key) {
        cache.invalidate(new CacheKey(locator, key));
    }

    private void databasePut(Locator locator, String key, String value) throws CacheException {
        try {
            io.put(locator, key, value);
        } catch (IOException ex) {
            throw new CacheException(ex);
        }
    }

    // implements the CacheLoader interface.
    private String databaseLoad(Locator locator, String key) throws CacheException {
        try {
            CacheKey cacheKey = new CacheKey(locator, key);
            Map<String, String> metadata = io.getAllValues(locator);
            if (metadata == null || metadata.isEmpty()) {
                cache.put(cacheKey, NULL);
                return NULL;
            }

            int metadataRowSize = 0;
            // prepopulate all other metadata other than the key we called the method with
            for (Map.Entry<String, String> meta : metadata.entrySet()) {
                metadataRowSize += meta.getKey().getBytes().length + locator.toString().getBytes().length;
                if (meta.getValue() != null)
                    metadataRowSize += meta.getValue().getBytes().length;
                if (meta.getKey().equals(key)) continue;
                CacheKey metaKey = new CacheKey(locator, meta.getKey());
                cache.put(metaKey, meta.getValue());
            }
            totalMetadataSize.update(metadataRowSize);

            String value = metadata.get(key);

            if (value == null) {
                cache.put(cacheKey, NULL);
                value = NULL;
            }

            return value;
        } catch (IOException ex) {
            throw new CacheException(ex);
        }
    }

    private void databaseLazyLoad(Locator locator) {
        boolean isPresent = outstandingMetaReads.contains(locator);

        if (!isPresent) {
            metaReads.add(locator);
            outstandingMetaReads.add(locator);
        }

        // Kickoff fetch meta if necessary
        if (metaReads.size() > batchedReadsThreshold) {
            fetchMeta(false);
        }
    }

    private void databaseLazyWrite(Locator locator, String metaKey) {
        CacheKey compoundKey = new CacheKey(locator, metaKey);
        if (outstandingMetaWrites.contains(compoundKey)) {
            return; // already queued up to write.
        }

        outstandingMetaWrites.add(compoundKey);
        metaWrites.add(compoundKey);

        if (metaWrites.size() > batchedWritesThreshold) {
            flushMeta(false);
        }

        return;
    }

    private void fetchMeta(boolean forced) { // Only one thread should ever call into this.
        synchronized (metaReads) {
            if (!forced && metaReads.size() < batchedReadsThreshold) {
                return;
            }

            while (!metaReads.isEmpty()) {
                Set<Locator> batch = new HashSet<Locator>();

                for (int i = 0; !metaReads.isEmpty() && i < batchedReadsThreshold; i++) {
                    batch.add(metaReads.poll()); // poll() is a destructive read (removes the head from the queue).
                }

                readThreadPoolExecutor.submit(new BatchedMetaReadsRunnable(batch));
            }
        }
    }

    private void flushMeta(boolean forced) { // Only one thread should ever call into this.
        synchronized (metaWrites) {
            if (!forced && metaWrites.size() < batchedWritesThreshold) {
                return;
            }

            while (!metaWrites.isEmpty()) {
                Table<Locator, String, String> metaBatch = HashBasedTable.create();

                for (int i = 0; !metaWrites.isEmpty() && i < batchedWritesThreshold; i++) {
                    CacheKey compoundKey = metaWrites.poll(); // destructive read.
                    Locator locator = compoundKey.locator();
                    String metaKey = compoundKey.keyString();
                    String metaVal = cache.getIfPresent(compoundKey);
                    if (metaVal != null) {
                        metaBatch.put(locator, metaKey, metaVal);
                    }
                }

                writeThreadPoolExecutor.submit(new BatchedMetaWritesRunnable(metaBatch));
            }
        }
    }

    private final class CacheKey implements Comparable<CacheKey> {
        private final Locator locator;
        private final String keyString;
        private final int hashCode;

        CacheKey(Locator locator, String keyString) {
            this.locator = locator;
            this.keyString = keyString;
            hashCode = (locator.toString() + "," + keyString).hashCode();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        public Locator locator() {
            return locator;
        }

        public String keyString() {
            return keyString;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CacheKey)) return false;
            CacheKey other = (CacheKey)obj;
            // kind of a cop-out.
            return (locator().equals(other.locator) && keyString().equals(other.keyString()));
        }

        @Override
        public String toString() {
            return locator.toString() + "," + keyString;
        }

        @Override
        public int compareTo(CacheKey o) {
            return this.toString().compareTo(o.toString());
        }
    }

    @Override
    public CacheStats getStats() {
        return cache.stats();
    }

    private class BatchedMetaReadsRunnable implements Runnable {
        private final Set<Locator> locators;

        public BatchedMetaReadsRunnable(Set<Locator> locators) {
            this.locators = locators;
        }

        @Override
        public void run() {
            Timer.Context ctx = batchedReadsTimerMetric.time();
            try {
                Table<Locator, String, String> metaTable = io.getAllValues(locators);
                int metadataRowSize = 0;

                for (Locator locator : metaTable.rowKeySet()) {
                    Map<String, String> metaMapForLocator = metaTable.row(locator);

                    for (Map.Entry<String, String> meta : metaMapForLocator.entrySet()) {
                        CacheKey metaKey = new CacheKey(locator, meta.getKey());
                        String existing = cache.getIfPresent(metaKey);

                        if (existing == null) {
                            cache.put(metaKey, meta.getValue());
                        }

                        boolean differs = existing != null && !existing.equals(meta.getValue());
                        if (differs) {
                            log.warn("Meta " + meta.getKey() + " changed from " + existing + " to " + meta.getValue()
                                    + " for locator " + locator); // delayed audit log.
                            // In this case, do not update the cache. DB has stale data.
                            continue;
                        }

                        metadataRowSize += meta.getKey().getBytes().length + locator.toString().getBytes().length;
                        metadataRowSize += meta.getValue().getBytes().length;
                    }

                    // Got the meta for locator. Remove this from the place holder.
                    outstandingMetaReads.remove(locator);
                }

                totalMetadataSize.update(metadataRowSize);
                // Kickoff fetch meta if necessary
                if (metaReads.size() > batchedReadsThreshold) {
                    fetchMeta(false);
                }
            } catch (Exception ex) {
                // Queue up the locators again (at the end)!
                for (Locator locator : locators) {
                    metaReads.add(locator);
                }
                log.error("Exception reading metadata from db (batched reads)", ex);
            } finally {
                ctx.stop();
            }
        }
    }

    private class BatchedMetaWritesRunnable implements Runnable {
        private final Table<Locator, String, String> metaToWrite;

        public BatchedMetaWritesRunnable(Table<Locator, String, String> metaToWrite) {
            this.metaToWrite = metaToWrite;
        }

        @Override
        public void run() {
            Timer.Context ctx = batchedWritesTimerMetric.time();
            try {
                io.putAll(metaToWrite);
            } catch (Exception ex) {
                log.error("Exception writing metadata to db (batched writes)", ex);
                // Queue up writes at the end.
                for (Locator locator : metaToWrite.rowKeySet()) {
                    Map<String, String> metaMapForLocator = metaToWrite.row(locator);

                    for (Map.Entry<String, String> meta : metaMapForLocator.entrySet()) {
                        CacheKey compoundKey = new CacheKey(locator, meta.getKey());
                        metaWrites.add(compoundKey);
                        // This is fine. We always read the latest value from the real cache. So we'll pull the latest
                        // value to write.
                    }
                }
            } finally {
                ctx.stop();
            }
        }
    }
}
