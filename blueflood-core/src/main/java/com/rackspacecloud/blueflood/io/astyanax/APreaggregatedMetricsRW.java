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

package com.rackspacecloud.blueflood.io.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.PreaggregatedRW;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Astyanax driver
 */
public class APreaggregatedMetricsRW extends AbstractMetricsRW implements PreaggregatedRW{

    public APreaggregatedMetricsRW(boolean isRecordingDelayedMetrics, Clock clock) {
        this.isRecordingDelayedMetrics = isRecordingDelayedMetrics;
        this.clock = clock;
    }

    /**
     * Inserts a collection of metrics to the metrics_preaggregated_full column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics) throws IOException {
        insertMetrics(metrics, Granularity.FULL);
    }

    /**
     * Inserts a collection of metrics to the correct column family based on
     * the specified granularity
     *
     * @param metrics
     * @param granularity
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics, Granularity granularity) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertMetrics(metrics, CassandraModel.getPreaggregatedColumnFamily(granularity), isRecordingDelayedMetrics, clock);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws IOException {
        try {
            AstyanaxWriter.getInstance().insertRollups(writeContexts);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locator
     * @param range
     * @param gran
     * @return
     */
    @Override
    public MetricData getDatapointsForRange(final Locator locator,
                                            Range range,
                                            Granularity gran) {
        return AstyanaxReader.getInstance().getDatapointsForRange(locator, range, gran);
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified list of {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locators
     * @param range
     * @param gran
     * @return
     */
    @Override
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators,
                                                          Range range,
                                                          Granularity gran) {
        return AstyanaxReader.getInstance().getDatapointsForRange(locators, range, gran);
    }

    /**
     * Fetches a {@link com.rackspacecloud.blueflood.types.Points} object for a
     * particular locator and rollupType from the specified column family and
     * range
     *
     * @param locator
     * @param rollupType
     * @param range
     * @param columnFamilyName
     * @param <T> the type of Rollup object
     * @return
     */
    @Override
    public <T extends Rollup> Points<T> getDataToRollup(final Locator locator,
                                                        RollupType rollupType,
                                                        Range range,
                                                        String columnFamilyName) throws IOException {
        ColumnFamily cf = CassandraModel.getColumnFamily(columnFamilyName);
        // a quick n dirty hack, this code will go away someday
        Granularity granularity = CassandraModel.getGranularity(cf);
        Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, granularity);
        return AstyanaxReader.getInstance().getDataToRoll(rollupClass, locator, range, cf);
    }
}
