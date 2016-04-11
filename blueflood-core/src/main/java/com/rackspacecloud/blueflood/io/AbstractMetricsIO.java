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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;

import java.util.Collection;

/**
 * This is base class of all MetricsIO classes that deals with persisting/reading
 * data from metrics_{granularity} and metrics_preaggregated_{granularity} column
 * family. This contains some utility methods used/shared amongst various
 * implementation/subclasses of MetricsIO.
 */
public abstract class AbstractMetricsIO implements MetricsIO {

    /**
     * The key of the metrics_preaggregated_* Column Families
     */
    public static final String KEY = "key";

    /**
     * The name of the first column
     */
    public static final String COLUMN1 = "column1";

    /**
     * The name of the value column
     */
    public static final String VALUE = "value";

    protected Multimap<Locator, IMetric> asMultimap(Collection<IMetric> metrics) {
        Multimap<Locator, IMetric> map = LinkedListMultimap.create();
        for (IMetric metric: metrics)
            map.put(metric.getLocator(), metric);
        return map;
    }
}
