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
import java.util.List;

public class MetricsCollection {
    private final List<Metric> metrics;

    public MetricsCollection() {
        this.metrics = new ArrayList<Metric>();
    }

    public void add(List<Metric> other) {
        metrics.addAll(other);
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public int size() {
        return metrics.size();
    }

    public static List<List<Metric>> getMetricsAsBatches(MetricsCollection collection, int partitions) {
        if (partitions <= 0) {
            partitions = 1;
        }

        int sizePerBatch = collection.size()/partitions + 1;

        return Lists.partition(collection.getMetrics(), sizePerBatch);
    }
}
