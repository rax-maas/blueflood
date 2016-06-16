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

import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * The purpose of this class is to encapsulate the difference
 * between normal/basic metrics vs preaggregated metrics from
 * the services code.
 */
public class MetricsRWDelegator {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRWDelegator.class);
    private AbstractMetricsRW basicMetricsRW;
    private AbstractMetricsRW preAggrMetricsRW;

    /**
     * Constructor
     */
    public MetricsRWDelegator() {
        this(IOContainer.fromConfig().getBasicMetricsRW(),
                IOContainer.fromConfig().getPreAggregatedMetricsRW());
    }

    /**
     * Constructor
     * @param basicMetricsRW
     * @param preAggrMetricsRW
     */
    @VisibleForTesting
    public MetricsRWDelegator(AbstractMetricsRW basicMetricsRW, AbstractMetricsRW preAggrMetricsRW) {
        this.basicMetricsRW = basicMetricsRW;
        this.preAggrMetricsRW = preAggrMetricsRW;
    }

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

    /**
     *
     * @param metrics
     */
    public void insertMetrics(List<IMetric> metrics) throws IOException {

        Collection<IMetric> simpleMetrics = new ArrayList<IMetric>();
        Collection<IMetric> preagMetrics = new ArrayList<IMetric>();
        for (IMetric metric : metrics) {
            if (metric instanceof Metric)
                simpleMetrics.add(metric);
            else if (metric instanceof PreaggregatedMetric)
                preagMetrics.add(metric);
            else {
                LOG.warn(String.format("Dont know how to insert metric of class=%s for locator=%s rollupType=%s collectionTime=%d",
                        metric.getClass().getSimpleName(), metric.getLocator(), metric.getRollupType(), metric.getCollectionTime()));
            }
        }

        if (simpleMetrics.size() > 0)
            basicMetricsRW.insertMetrics(simpleMetrics);

        if (preagMetrics.size() > 0)
            preAggrMetricsRW.insertMetrics(preagMetrics);

    }
}
