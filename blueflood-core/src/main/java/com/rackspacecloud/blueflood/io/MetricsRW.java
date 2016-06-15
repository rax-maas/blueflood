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

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.RollupType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This is an interface defining behavior of reading/writing
 * metrics to various data stores.
 */
public interface MetricsRW {

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} objects
     * to the appropriate Cassandra column family.
     *
     * @param metrics
     *
     * @throws IOException
     */
    public void insertMetrics(Collection<IMetric> metrics) throws IOException;

    /**
     * This method inserts a collection of metrics in the
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
     * objects to the appropriate Cassandra column family
     *
     * @param writeContexts
     * @throws IOException
     */
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws IOException;

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
    public MetricData getDatapointsForRange(final Locator locator, Range range, Granularity gran);

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
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran);

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
    public <T extends Rollup> Points<T> getDataToRollup(final Locator locator, RollupType rollupType, Range range, String columnFamilyName) throws IOException;
}
