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

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;

import java.io.IOException;
import java.util.Collection;

/**
 * This is an interface defining behavior of reading/writing
 * metrics to various data stores.
 */
public interface MetricsIO {

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} objects
     * to the appropriate Cassandra column family.
     *
     * @param metrics
     * @throws IOException
     */
    public void insertMetrics(Collection<IMetric> metrics) throws IOException;

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} rolled up
     * objects to the appropriate Cassandra column family
     *
     * @param metrics
     * @param granularity
     * @throws IOException
     */
    public void insertRollups(Collection<IMetric> metrics, Granularity granularity) throws IOException;

    // TODO: to be written when we wire everything together in PreaggregatedMetricsIO
    // public MetricData getDatapointsForRange(Locator locator, Range range, Granularity gran);
    // public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran);
    // public <T extends Rollup> Points<T> getDataToRoll(Class<T> type, final Locator locator, Range range, ColumnFamily<Locator, Long> cf)
}
