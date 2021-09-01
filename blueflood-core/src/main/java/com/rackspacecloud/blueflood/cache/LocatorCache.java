package com.rackspacecloud.blueflood.cache;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class is used to cache locator's that were written recently to our persistence layers by the available writers.
 * All the writers, check the cache to see if it is written recently(isCurrent) before writing them again.
 *
 * Different writers that we have:
 *
 *  {@link com.rackspacecloud.blueflood.inputs.processors.BatchWriter} This writes to cassandra
 *  {@link com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter} This supports metric discovery (/metric/search)
 *  {@link com.rackspacecloud.blueflood.inputs.processors.TokenDiscoveryWriter} This support metric tokens discovery (metric_name/search)
 *
 * Each writer maintains its own marker in the cache to indicate whether a locator is current. This
 * is useful in cases, where persisting a locator with one writer is successful but not with other writers.
 *
 */
public class LocatorCache {

    // this collection is used to reduce the number of locators that get written.
    // Simply, if a locator has been seen within the last 10 minutes, don't bother.
    private final Cache<String, Boolean> insertedLocators;

    // this collection is used to reduce the number of delayed locators that get
    // written per slot. Simply, if a locator has been seen for a slot, don't bother.
    private final Cache<String, Boolean> insertedDelayedLocators;

    private final static LocatorCache instance = new LocatorCache(10, TimeUnit.MINUTES, 3, TimeUnit.DAYS);

    public enum Layer { BATCH, DISCOVERY, TOKEN_DISCOVERY }

    static {
        Metrics.getRegistry().register(MetricRegistry.name(LocatorCache.class, "Current Locators Count"),
                (Gauge<Long>) instance::getCurrentLocatorCount);

        Metrics.getRegistry().register(MetricRegistry.name(LocatorCache.class, "Current Delayed Locators Count"),
                (Gauge<Long>) instance::getCurrentDelayedLocatorCount);
    }

    public static LocatorCache getInstance() {
        return instance;
    }

    protected LocatorCache(long expireAfterAccessDuration, TimeUnit expireAfterAccessTimeUnit,
                           long expireAfterWriteDuration, TimeUnit expireAfterWriteTimeUnit) {
        int concurrency = Configuration.getInstance().getIntegerProperty(CoreConfig.LOCATOR_CACHE_CONCURRENCY);
        insertedLocators =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(expireAfterAccessDuration, expireAfterAccessTimeUnit)
                        .expireAfterWrite(expireAfterWriteDuration, expireAfterWriteTimeUnit)
                        .concurrencyLevel(concurrency)
                        .build();

        insertedDelayedLocators =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(expireAfterAccessDuration, expireAfterAccessTimeUnit)
                        .expireAfterWrite(expireAfterWriteDuration, expireAfterWriteTimeUnit)
                        .concurrencyLevel(concurrency)
                        .build();
    }

    @VisibleForTesting
    public static LocatorCache getInstance(long expireAfterAccessDuration, TimeUnit expireAfterAccessTimeUnit,
                                           long expireAfterWriteDuration, TimeUnit expireAfterWriteTimeUnit) {

        return new LocatorCache(expireAfterAccessDuration, expireAfterAccessTimeUnit,
                                expireAfterWriteDuration, expireAfterWriteTimeUnit);
    }

    public long getCurrentLocatorCount() {
        return insertedLocators.size();
    }

    public long getCurrentDelayedLocatorCount() {
        return insertedDelayedLocators.size();
    }

    /**
     * Checks if Locator is recently inserted in the batch layer
     */
    public boolean isLocatorCurrentInBatchLayer(Locator loc) {
        return isLocatorCurrentInLayer(loc, Layer.BATCH);
    }

    /**
     * Checks if Locator is recently inserted in the discovery layer
     */
    public boolean isLocatorCurrentInDiscoveryLayer(Locator loc) {
        return isLocatorCurrentInLayer(loc, Layer.DISCOVERY);
    }

    /**
     * Checks if Locator is recently inserted in the token discovery layer
     */
    public boolean isLocatorCurrentInTokenDiscoveryLayer(Locator loc) {
        return isLocatorCurrentInLayer(loc, Layer.TOKEN_DISCOVERY);
    }

    /**
     * Check if the delayed locator is recently inserted for a given slot
     */
    public boolean isDelayedLocatorForASlotCurrent(int slot, Locator locator) {
        return insertedDelayedLocators.getIfPresent(getLocatorSlotKey(slot, locator)) != null;
    }

    private String getLocatorSlotKey(int slot, Locator locator) {
        return slot + "," + locator.toString();
    }

    /**
     * Marks the Locator as recently inserted in the batch layer
     */
    public void setLocatorCurrentInBatchLayer(Locator loc) {
        setLocatorCurrentInLayer(loc, Layer.BATCH);
    }

    /**
     * Marks the Locator as recently inserted in the discovery layer
     */
    public void setLocatorCurrentInDiscoveryLayer(Locator loc) {
        setLocatorCurrentInLayer(loc, Layer.DISCOVERY);
    }

    /**
     * Marks the Locator as recently inserted in the token discovery layer
     */
    public void setLocatorCurrentInTokenDiscoveryLayer(Locator loc) {
        setLocatorCurrentInLayer(loc, Layer.TOKEN_DISCOVERY);
    }

    /**
     * Marks the delayed locator as recently inserted for a given slot
     */
    public void setDelayedLocatorForASlotCurrent(int slot, Locator locator) {
        insertedDelayedLocators.put(getLocatorSlotKey(slot, locator), Boolean.TRUE);
    }

    @VisibleForTesting
    public void resetCache() {
        insertedLocators.invalidateAll();
        insertedDelayedLocators.invalidateAll();
    }

    @VisibleForTesting
    public void resetInsertedLocatorsCache() {
        insertedLocators.invalidateAll();
    }

    /**
     * Checks if a locator is cached in the given layer. It works like other isLocatorCurrentIn* methods but accepts
     * the "layer" as an argument instead of as part of the method name.
     * @param locator value to check for in the cache
     * @param layer the layer that the locator value must be in
     * @return true if the locator is set in the layer, else false
     */
    public boolean isLocatorCurrentInLayer(Locator locator, Layer layer) {
        return insertedLocators.getIfPresent(toCacheKey(locator, layer)) != null;
    }

    /**
     * Sets a locator as current in a given layer. It works like other setLocatorCurrentIn* methods but accepts the
     * "layer" as an argument instead of as part of the method name.
     * @param locator value to set as current in the cache
     * @param layer the layer in which to set it current
     */
    public void setLocatorCurrentInLayer(Locator locator, Layer layer) {
        insertedLocators.put(toCacheKey(locator, layer), true);
    }

    private String toCacheKey(Locator locator, Layer layer) {
        return layer.name() + "." + locator.toString();
    }

    /**
     * Gets all locators that are current in the given layer. Use with caution, as this could be a huge list.
     * Recommended only for use in testing.
     */
    public List<Locator> getAllCurrentInLayer(Layer layer) {
        String prefix = layer.name() + ".";
        return insertedLocators.asMap().keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .map(key -> key.replaceFirst(prefix, ""))
                .map(Locator::createLocatorFromDbKey)
                .collect(Collectors.toList());
    }
}
