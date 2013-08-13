package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.RollupMetric;
import telescope.thrift.RollupMetrics;

import java.util.HashSet;
import java.util.Set;

public class DefaultThriftOutputSerializerTest {
    private static final Set<String> defaultStats;

    static {
        defaultStats = new HashSet<String>();
        defaultStats.add("average");
        defaultStats.add("variance");
        defaultStats.add("min");
        defaultStats.add("max");
    }

    @Test
    public void testTransformRollupDataAtFullRes() throws Exception {
        final DefaultThriftOutputSerializer serializer = new DefaultThriftOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeFullResPoints(), "unknown");
        RollupMetrics metrics = serializer.transformRollupData(metricData, defaultStats);

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
    public void testTransformRollupDataForCoarserGran() throws Exception {
        final DefaultThriftOutputSerializer serializer = new DefaultThriftOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeRollupPoints(), "unknown");

        RollupMetrics metrics = serializer.transformRollupData(metricData, defaultStats);

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
}
