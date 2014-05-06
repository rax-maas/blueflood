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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MetadataCache extends AbstractJmxCache implements MetadataCacheMBean {
    // todo: give each cache a name.

    private final com.google.common.cache.LoadingCache<CacheKey, String> cache;
    private MetadataIO io = new AstyanaxMetadataIO();
    private static final String NULL = "null".intern();
    private static final Logger log = LoggerFactory.getLogger(MetadataCache.class);
    private static final TimeValue defaultExpiration = new TimeValue(Configuration.getInstance().getIntegerProperty(CoreConfig.META_CACHE_RETENTION_IN_MINUTES), TimeUnit.MINUTES);
    private static final int defaultConcurrency = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_SCRIBE_WRITE_THREADS);
    private static final MetadataCache INSTANCE = new MetadataCache(defaultExpiration, defaultConcurrency);
    private static final Counter nullsInCache = Metrics.counter(MetadataCache.class, "Putting Nulls in cache");
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

    public boolean containsKey(Locator locator, String key) {
        return cache.getIfPresent(new CacheKey(locator, key)) != null;
    }

    public String get(Locator locator, String key) throws CacheException {
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
            return (T)get(locator, key);
        } catch (ClassCastException ex) {
            throw new CacheException(ex);
        }
    }

    // todo: synchronization?
    // returns true if updated.
    public boolean put(Locator locator, String key, String value) throws CacheException {
        if (value == null) return false;
        Timer.Context cachePutTimerContext = MetadataCache.cachePutTimer.time();
        try {
            CacheKey cacheKey = new CacheKey(locator, key);
            String oldValue = cache.getIfPresent(cacheKey);
            // don't care if oldValue == EMPTY.
            // always put new value in the cache. it keeps reads from happening.
            cache.put(cacheKey, value);
            if (oldValue == null || !oldValue.equals(value)) {
                updatedMetricMeter.mark();
                databasePut(locator, key, value);
                return true;
            } else {
                return false;
            }
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
                nullsInCache.inc();
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
                nullsInCache.inc();
                value = NULL;
            }

            return value;
        } catch (IOException ex) {
            throw new CacheException(ex);
        }
    }

    private final class CacheKey {
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
    }

    @Override
    public CacheStats getStats() {
        return cache.stats();
    }
}
