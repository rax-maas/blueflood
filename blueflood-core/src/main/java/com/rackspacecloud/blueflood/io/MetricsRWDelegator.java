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

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricMetadata;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.RollupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The purpose of this class is to encapsulate the difference
 * between normal/basic metrics vs preaggregated metrics from
 * the services code.
 */
public class MetricsRWDelegator {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRWDelegator.class);

    /**
     *
     * @param locator
     * @param range
     * @param gran
     * @return
     */
    public MetricData getDatapointsForRange(final Locator locator, Range range, Granularity gran) {
        return getDatapointsForRange(new ArrayList<Locator>() {{ add(locator); }}, range, gran).get(locator);
    }

    /**
     *
     * @param locators
     * @param range
     * @param gran
     * @return
     */
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran) {

        MetadataCache metadataCache = MetadataCache.getInstance();
        AbstractMetricsRW basicMetricsRW = IOContainer.fromConfig().getBasicMetricsRW();
        AbstractMetricsRW preAggrMetricsRW = IOContainer.fromConfig().getPreAggregatedMetricsRW();
        EnumMetricData enumMetricData = new EnumMetricData(IOContainer.fromConfig().getEnumReaderIO());

        List<Locator> basicLocators = new ArrayList<Locator>();
        List<Locator> preAggrLocators = new ArrayList<Locator>();
        List<Locator> enumLocators = new ArrayList<Locator>();
        for ( Locator locator : locators ) {
            try {
                RollupType rollupType = RollupType.fromString(
                        metadataCache.get(locator,
                                MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));

                if (rollupType == RollupType.COUNTER ||
                        rollupType == RollupType.GAUGE ||
                        rollupType == RollupType.SET ||
                        rollupType == RollupType.TIMER) {
                    preAggrLocators.add(locator);
                } else if ( rollupType == RollupType.ENUM ) {
                    // enum has to be handled specially, because of
                    // the join of 2 CFs
                    enumLocators.add(locator);
                } else {
                    basicLocators.add(locator);
                }
            } catch (CacheException ex) {
                // pass for now. need metric to figure this stuff out.
                LOG.error(String.format("cant lookup rollupType for locator %s, range %s, granularity %s",
                        locator, range.toString(), gran.toString()), ex);
            }
        }

        // combine all the result
        Map<Locator, MetricData> result = new HashMap<Locator, MetricData>();
        if ( ! basicLocators.isEmpty() ) {
            result.putAll(basicMetricsRW.getDatapointsForRange(basicLocators, range, gran));
        }

        if ( ! enumLocators.isEmpty() ) {
            result.putAll(enumMetricData.getEnumMetricDataForRangeForLocatorList(enumLocators, range, gran));
        }

        if ( ! preAggrLocators.isEmpty() ) {
            result.putAll(preAggrMetricsRW.getDatapointsForRange(preAggrLocators, range, gran));
        }
        return result;
    }
}
