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

    private final static LocatorCache instance = new LocatorCache(
            Configuration.getInstance().getIntegerProperty(CoreConfig.LOCATOR_CACHE_TTL_MINUTES),
            TimeUnit.MINUTES,
            Configuration.getInstance().getIntegerProperty(CoreConfig.LOCATOR_CACHE_DELAYED_TTL_SECONDS),
            TimeUnit.SECONDS);

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

    protected LocatorCache(long entryTtl, TimeUnit entryTtlTimeUnit,
                           long delayedEntryTtl, TimeUnit delayedEntryTtlTimeUnit) {
        int concurrency = Configuration.getInstance().getIntegerProperty(CoreConfig.LOCATOR_CACHE_CONCURRENCY);
        // Note: DO NOT use expireAfterWrite at the same time as expireAfterAccess. The latter expires entries that are
        // written and never touched again on its own. If you combine them, the entries WILL expire after the
        // expireAfterWrite duration, even if they're constantly being accessed.
        insertedLocators =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(entryTtl, entryTtlTimeUnit)
                        .concurrencyLevel(concurrency)
                        .build();

        // The settings for the delayed locator cache used to be shared with the normal locator cache, but I suspect
        // that can cause problems when you set longer durations for the other cache. If the delayed locator cache
        // holds entries for too long, it would prevent us from updating cassandra with legitimate delayed locators,
        // and therefore the data won't get rolled up when it should. Under normal circumstances, delayed locators
        // should be a fairly rare occurrence. They'll mostly happen during some kind of outage, when a backlog of
        // data builds up in upstream systems. Then they'll be tightly clustered as the backlog is processed, so a
        // relatively short TTL should work fine here.
        //
        // Instead of worrying about this, shouldn't delayed locators be handled the same way as normal ones, via
        // the ShardStateManager? I'm not clear enough on delayed rollups to give a definitive answer there.
        insertedDelayedLocators =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(delayedEntryTtl, delayedEntryTtlTimeUnit)
                        .concurrencyLevel(concurrency)
                        .build();
    }

    @VisibleForTesting
    public static LocatorCache getInstance(long entryTtl, TimeUnit entryTtlTimeUnit,
                                           long delayedEntryTtl, TimeUnit delayedEntryTtlTimeUnit) {
        return new LocatorCache(entryTtl, entryTtlTimeUnit, delayedEntryTtl, delayedEntryTtlTimeUnit);
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
     * Checks if Locator is recently inserted in the token discovery layer.
     * Token discovery is no longer a thing. This is no longer used and will be removed.
     */
    @Deprecated
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
     * Token discovery is no longer a thing. This is no longer used and will be removed.
     */
    @Deprecated
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
