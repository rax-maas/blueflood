package com.cloudkick.blueflood.outputs.serializers;

import com.cloudkick.blueflood.outputs.formats.MetricData;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Points;
import com.cloudkick.blueflood.types.Rollup;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.RollupMetric;
import telescope.thrift.RollupMetrics;

public class DefaultThriftOutputSerializerTest {

    @Test
    public void testTransformRollupDataAtFullRes() {
        final DefaultThriftOutputSerializer serializer = new DefaultThriftOutputSerializer();
        final MetricData metricData = new MetricData(generateFakeFullResPoints(), "unknown");
        RollupMetrics metrics = serializer.transformRollupData(metricData);

        // Assert unit is same
        Assert.assertEquals(metricData.getUnit(), metrics.getUnit());

        // Assert that for each data point, the thrift value matches the value in Point
        for (RollupMetric rollupMetric : metrics.getMetrics()) {
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(rollupMetric.getTimestamp());
            Assert.assertEquals(point.getData(),
                    rollupMetric.getRawSample().getValueI64());
        }
    }


    @Test
    public void testTransformRollupDataForCoarserGran() {
        final DefaultThriftOutputSerializer serializer = new DefaultThriftOutputSerializer();
        final MetricData metricData = new MetricData(generateFakeRollupPoints(), "unknown");

        RollupMetrics metrics = serializer.transformRollupData(metricData);

        // Assert unit is same
        Assert.assertEquals(metricData.getUnit(), metrics.getUnit());

        // Assert that for each data point, the thrift value matches the value in Point
        for (RollupMetric rollupMetric : metrics.getMetrics()) {
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(rollupMetric.getTimestamp());
            final Rollup rollup = (Rollup) point.getData();
            Assert.assertEquals(((Rollup) point.getData()).getAverage().toLong(),
                    rollupMetric.getAverage().getValueI64());
            Assert.assertEquals(((Rollup) point.getData()).getCount(),
                    rollupMetric.getNumPoints());
        }
    }

    private Points<Long> generateFakeFullResPoints() {
        Points<Long> points = Points.create(Granularity.FULL);

        long baseTime = 1234567L;
        for (int count = 0; count < 5; count++) {
            Points.Point<Long> point = new Points.Point<Long>(baseTime + (count*1000), (long) count);
            points.add(point);
        }

        return points;
    }

    private Points<Rollup> generateFakeRollupPoints() {
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
