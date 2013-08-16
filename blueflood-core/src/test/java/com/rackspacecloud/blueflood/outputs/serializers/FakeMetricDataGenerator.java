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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;

public class FakeMetricDataGenerator {
    public static Points<Long> generateFakeFullResPoints() {
        Points<Long> points = Points.create(Granularity.FULL);

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            Points.Point<Long> point = new Points.Point<Long>(baseTime + (count*1000), (long) count);
            points.add(point);
        }

        return points;
    }

    public static Points<Rollup> generateFakeRollupPoints() {
        Points<Rollup> points = Points.create(Granularity.MIN_5);

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            final Rollup rollup = new Rollup();
            rollup.setCount(count * 100);
            rollup.getAverage().setLongValue(count);
            Points.Point<Rollup> point = new Points.Point<Rollup>(baseTime + (count*1000), rollup);
            points.add(point);
        }

        return points;
    }
    
    public static Points<String> generateFakeStringPoints() {
        Points<String> points = Points.create(Granularity.FULL);
        long startTime = 1234567L;
        for (int i =0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<String> point = new Points.Point<String>(timeNow, String.valueOf(timeNow));
        }
        return points;
    }
}
