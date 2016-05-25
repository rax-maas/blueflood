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

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.RollupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities to operate on {@link com.rackspacecloud.blueflood.types.Locator} objects
 */
public class LocatorsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LocatorsUtils.class);
    private static final MetadataCache metadataCache = MetadataCache.getInstance();

    private static final String rollupTypeCacheKey = MetricMetadata.ROLLUP_TYPE.toString().toLowerCase();

    public static List<String> toStringList(final List<Locator> locators) {
        List<String> locatorStrs = new ArrayList<String>() {{
            for (Locator locator : locators) {
                add(locator.toString());
            }
        }};
        return locatorStrs;
    }

    /**
     * For a particular locator, determine what is the proper class to do
     * read/write with and return that
     *
     * @param locator
     * @return
     */
    public static AbstractMetricsRW getMetricsRWForLocator(Locator locator) throws CacheException {

        RollupType rollupType = RollupType.fromString(metadataCache.get(locator, rollupTypeCacheKey));

        if (rollupType == null || rollupType == RollupType.BF_BASIC ) {
            return IOContainer.fromConfig().getBasicMetricsRW();
        } else {
            return IOContainer.fromConfig().getPreAggregatedMetricsRW();
        }
    }
}
