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
