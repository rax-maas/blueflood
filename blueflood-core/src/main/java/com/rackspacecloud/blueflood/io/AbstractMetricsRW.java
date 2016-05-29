/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.cache.SafetyTtlProvider;
import com.rackspacecloud.blueflood.cache.TenantTtlProvider;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is base class of all MetricsRW classes that deals with persisting/reading
 * data from metrics_string, metrics_{granularity} and metrics_preaggregated_{granularity} column
 * family. This contains some utility methods used/shared amongst various
 * implementation/subclasses of MetricsRW.
 */
public abstract class AbstractMetricsRW implements MetricsRW {

    protected static final MetadataCache metadataCache = MetadataCache.getInstance();
    protected static final String DATA_TYPE_CACHE_KEY = MetricMetadata.TYPE.toString().toLowerCase();

    protected static TenantTtlProvider TTL_PROVIDER = SafetyTtlProvider.getInstance();

    // this collection is used to reduce the number of locators that get written.
    // Simply, if a locator has been seen within the last 10 minutes, don't bother.
    protected static final Cache<String, Boolean> insertedLocators =
            CacheBuilder.newBuilder().expireAfterAccess(10,
                        TimeUnit.MINUTES).concurrencyLevel(16).build();

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricsRW.class);

    private static final long ROLLUP_DELAY_MILLIS = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);

    private final Clock clock;

    protected AbstractMetricsRW(Clock clock) {
        this.clock = clock;
    }

    /**
     * Returns true if the metric is delayed with respect to current time and ROLLUP_DELAY_MILLIS configuration.
     *
     * @param metric
     * @return
     */
    public boolean isDelayedMetric(IMetric metric) {
        long timeElapsed = clock.now().getMillis() - metric.getCollectionTime();
        return timeElapsed > ROLLUP_DELAY_MILLIS;
    }

    /**
     * Checks if Locator is recently inserted
     *
     * @param loc
     * @return
     */
    protected synchronized boolean isLocatorCurrent(Locator loc) {
        return insertedLocators.getIfPresent(loc.toString()) != null;
    }

    /**
     * Marks the Locator as recently inserted
     * @param loc
     */
    protected synchronized void setLocatorCurrent(Locator loc) {
        insertedLocators.put(loc.toString(), Boolean.TRUE);
    }

    /**
     * Convert a collection of {@link com.rackspacecloud.blueflood.types.IMetric}
     * to a {@link com.google.common.collect.Multimap}
     *
     * @param metrics
     * @return
     */
    protected Multimap<Locator, IMetric> asMultimap(Collection<IMetric> metrics) {
        Multimap<Locator, IMetric> map = LinkedListMultimap.create();
        for (IMetric metric: metrics)
            map.put(metric.getLocator(), metric);
        return map;
    }

    /**
     * For a particular {@link com.rackspacecloud.blueflood.types.Locator}, get
     * its corresponding {@link com.rackspacecloud.blueflood.types.DataType}
     *
     * @param locator
     * @param dataTypeCacheKey
     * @return
     * @throws CacheException
     */
    protected DataType getDataType(Locator locator, String dataTypeCacheKey) throws CacheException {
        String meta = metadataCache.get(locator, dataTypeCacheKey);
        if (meta != null) {
            return new DataType(meta);
        }
        return DataType.NUMERIC;
    }

    // TODO: can this move to MetadataCache?
    protected String getUnitString(Locator locator) {
        String unitString = Util.UNKNOWN;
        // Only grab units from cassandra, if we have to
        if (!Util.shouldUseESForUnits()) {
            try {
                unitString = metadataCache.get(locator, MetricMetadata.UNIT.name().toLowerCase(), String.class);
            } catch (CacheException ex) {
                LOG.warn("Cache exception reading unitString from MetadataCache: ", ex);
            }
            if (unitString == null) {
                unitString = Util.UNKNOWN;
            }
        }
        return unitString;
    }

    /**
     * Gets the TTL for a particular locator, rollupType and granularity.
     *
     * @param locator
     * @param rollupType
     * @param granularity
     * @return
     */
    protected int getTtl(Locator locator, RollupType rollupType, Granularity granularity) {
        try {
            return (int) TTL_PROVIDER.getTTL(locator.getTenantId(),
                    granularity,
                    rollupType).toSeconds();
        } catch (Exception ex) {
            LOG.warn(String.format("error getting TTL for locator %s, granularity %s, defaulting to safe TTL",
                    locator, granularity), ex);
            return (int) SafetyTtlProvider.getInstance().getSafeTTL(granularity, rollupType).toSeconds();
        }
    }

    /**
     * Converts the map of timestamp -> {@link Rollup} to
     * {@link Points} object
     *
     * @param timestampToRollupMap  a map of timestamp to rollup
     * @return
     */
    protected <T extends Object> Points<T> convertToPoints(final Map<Long, T> timestampToRollupMap) {
        Points points =  new Points();
        for (Map.Entry<Long, T> value : timestampToRollupMap.entrySet() ) {

            points.add( createPoint( value.getKey(), value.getValue() ) );
        }
        return points;
    }

    protected Points.Point createPoint( Long timestamp, Object value ) {
        if( value instanceof Rollup )
            return new Points.Point( timestamp, value);
        else
            return new Points.Point( timestamp, new SimpleNumber( value ) );
    }
}
