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

import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.SimpleNumber;

public class FakeMetricDataGenerator {
    public static Points<SimpleNumber> generateFakeFullResPoints() {
        Points<SimpleNumber> points = new Points<SimpleNumber>();

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            Points.Point<SimpleNumber> point = new Points.Point<SimpleNumber>(baseTime + (count*1000), new SimpleNumber((long) count));
            points.add(point);
        }

        return points;
    }

    public static Points<BasicRollup> generateFakeRollupPoints() {
        Points<BasicRollup> points = new Points<BasicRollup>();

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            final BasicRollup basicRollup = new BasicRollup();
            basicRollup.setCount(count * 100);
            basicRollup.getAverage().setLongValue(count);
            Points.Point<BasicRollup> point = new Points.Point<BasicRollup>(baseTime + (count*1000), basicRollup);
            points.add(point);
        }

        return points;
    }

    public static Points<String> generateFakeStringPoints() {
        Points<String> points = new Points<String>();
        long startTime = 1234567L;
        for (int i =0; i < 5; i++) {
            long timeNow = startTime + i*1000;
            Points.Point<String> point = new Points.Point<String>(timeNow, String.valueOf(timeNow));
        }
        return points;
    }
}
