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
}
