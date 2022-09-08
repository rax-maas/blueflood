package com.rackspacecloud.blueflood.cache;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.util.concurrent.TimeUnit;

/**
 * Caches tokens that have been written to Elasticsearch. Like the {@link LocatorCache}, this is a memory cache that
 * guards Elasticsearch from excessive writes during ingestion. When a token is successfully indexed, it's cached here.
 * If a token is present in the cache, it won't be written to Elasticsearch again. This functionality is implemented in
 * ElasticTokensIO.
 *
 * "Tokens" are the separate, dot-separated pieces of a hierarchical metric name. See {@link Token} for more details.
 * ElasticTokensIO caches only the non-leaf tokens under the assumption that locator caching/guarding happens
 * beforehand, specifically in the {@link com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter}. If a locator
 * has already been run through discovery, all its tokens will have been processed as well.
 *
 * Since the locator cache is checked before processing tokens, it might seem that this cache is redundant. In the
 * special case of a node start/restart, however, every locator appears new, and a huge number of tokens is generated.
 * At that time, this cache is heavily used and greatly reduces the number of tokens that need to be indexed. It's also
 * worth pointing out that after that initial burst of activity, the entries in this cache will begin expiring out and
 * won't be replaced.
 *
 * If a locator is present in the locator cache, then we won't attempt to index tokens for it either. This is good, but
 * in the case of a perfect locator cache, it means no calls will ever reach the token cache. Unfortunately, the current
 * cache implementation only removes expired entries when you actually access the cache, so this situation can lead to a
 * lot of tokens sitting in the token cache that are never cleaned out. If you see high usage in the token cache after
 * its TTL, it probably means you're in this situation.
 *
 * TODO: Fix the issue described above. One option is finding another cache implementation that will actively remove
 *       expired entries. Another option would be starting a thread to periodically run a cleanup on the cache. Note
 *       that the caches used for throttling in DiscoveryWriter have a similar problem and could benefit from periodic
 *       cleanups instead of the inline cleanups of the current implementation
 *
 * See the LOCATOR_CACHE_* and TOKEN_CACHE_* setting in {@link CoreConfig} to tune the caches.
 */
public class TokenCache {

    // this collection is used to reduce the number of tokens that get written.
    // Simply, if a token has been seen within the last 10 minutes, don't bother.
    private final Cache<String, Boolean> insertedTokens;

    private static TokenCache instance = new TokenCache(
            Configuration.getInstance().getIntegerProperty(CoreConfig.TOKEN_CACHE_TTL_MINUTES),
            TimeUnit.MINUTES);


    static {
        Metrics.getRegistry().register(MetricRegistry.name(TokenCache.class, "Current Tokens Count"),
                (Gauge<Long>) instance::getCurrentLocatorCount);
    }

    public static TokenCache getInstance() {
        return instance;
    }

    protected TokenCache(long entryTtl, TimeUnit entryTtlTimeUnit) {
        insertedTokens =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(entryTtl, entryTtlTimeUnit)
                        .concurrencyLevel(16)
                        .build();
    }

    @VisibleForTesting
    public static TokenCache getInstance(long entryTtl, TimeUnit entryTtlTimeUnit) {
        return new TokenCache(entryTtl, entryTtlTimeUnit);
    }

    public long getCurrentLocatorCount() {
        return insertedTokens.size();
    }

    /**
     * Checks if token is recently inserted
     *
     */
    public boolean isTokenCurrent(Token token) {
        return insertedTokens.getIfPresent(token.getId()) != null;
    }

    /**
     * Marks the token as recently inserted
     */
    public void setTokenCurrent(Token token) {
        insertedTokens.put(token.getId(), Boolean.TRUE);
    }

    @VisibleForTesting
    public void resetCache() {
        insertedTokens.invalidateAll();
    }

}
