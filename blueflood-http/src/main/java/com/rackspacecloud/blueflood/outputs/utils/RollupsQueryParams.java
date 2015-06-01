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

package com.rackspacecloud.blueflood.outputs.utils;

import com.rackspacecloud.blueflood.outputs.serializers.BasicRollupsOutputSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Resolution;

import java.util.Set;

public class RollupsQueryParams {
    private int points;
    private Resolution resolution;
    private final Range range;
    private final Set<BasicRollupsOutputSerializer.MetricStat> stats;
    private boolean isPoints = false;

    private RollupsQueryParams(long from, long to, Set<BasicRollupsOutputSerializer.MetricStat> stats) {
        if (from >= to) {
            throw new IllegalArgumentException("'from' timestamp has to be strictly less than 'to'.");
        }
        this.stats = stats;
        this.range = new Range(from, to);
        this.points = 0;
        this.resolution = Resolution.FULL;
    }

    public RollupsQueryParams(long from, long to, int points, Set<BasicRollupsOutputSerializer.MetricStat> stats) {
        this(from, to, stats);
        this.isPoints = true;
        this.points = points;
    }

    public RollupsQueryParams(long from, long to, Resolution resolution, Set<BasicRollupsOutputSerializer.MetricStat> stats) {
        this(from, to, stats);
        this.resolution = resolution;
        this.isPoints = false;
    }

    public boolean isGetByPoints() {
        return isPoints;
    }

    public boolean isGetByResolution() {
        return !isPoints;
    }

    public Granularity getGranularity(String tenantId) {
        if (isPoints) {
            return Granularity.granularityFromPointsInInterval(tenantId, range.getStart(), range.getStop(), points);
        } else {
            return Granularity.granularities()[resolution.getValue()];
        }
    }

    public Range getRange() {
        return range;
    }

    public int getPoints() {
        return points;
    }

    public Resolution getResolution() {
        return resolution;
    }

    public Set<BasicRollupsOutputSerializer.MetricStat> getStats() {
        return stats;
    }
}
