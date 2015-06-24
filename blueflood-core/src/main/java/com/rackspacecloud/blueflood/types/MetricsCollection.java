/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.types;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MetricsCollection {
    private final List<IMetric> metrics;

    public MetricsCollection() {
        this.metrics = new ArrayList<IMetric>();
    }

    public void add(Collection<IMetric> other) {
        metrics.addAll(other);
    }

    public Collection<IMetric> toMetrics() {
        return metrics;
    }

    public int size() {
        return metrics.size();
    }

    public List<List<IMetric>> splitMetricsIntoBatches(int sizePerBatch) {
        if (sizePerBatch <= 0) {
            sizePerBatch = metrics.size();
        }
        return Lists.partition(metrics, sizePerBatch);
    }
}
