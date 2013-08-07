package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.CloudMonitoringUtils;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import telescope.thrift.RollupMetric;
import telescope.thrift.RollupMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultThriftOutputSerializer implements OutputSerializer<RollupMetrics> {

    @Override
    public RollupMetrics transformRollupData(MetricData metricData) {
        final Points points = metricData.getData();
        return new RollupMetrics(transformPoints(points), metricData.getUnit());
    }

    public List<RollupMetric> transformPoints(Points points) {
        final List<RollupMetric> rollupMetricsList = new ArrayList<RollupMetric>();

        final Set<Map.Entry<Long, Points.Point>> data = points.getPoints().entrySet();
        for (Map.Entry<Long, Points.Point> item : data) {
            final RollupMetric rollupMetric = transformPointToRollupMetric(item.getValue());
            rollupMetricsList.add(rollupMetric);
        }

        return rollupMetricsList;
    }

    public RollupMetric transformPointToRollupMetric(Points.Point point) {
        RollupMetric rollupMetric;

        if (point.getData() instanceof Rollup) {
            rollupMetric = buildRollupThriftMetricFromRollup((Rollup) point.getData());
        } else {
            rollupMetric = buildRollupThriftMetricFromObject(point.getData());
        }
        rollupMetric.setTimestamp(point.getTimestamp());


        return rollupMetric;
    }

    public static RollupMetric buildRollupThriftMetricFromRollup(Rollup rollup) {
        RollupMetric rm = new RollupMetric();
        rm.setNumPoints(rollup.getCount());

        telescope.thrift.Metric m = CloudMonitoringUtils.createMetric(rollup.getAverage());
        rm.setAverage(m);
        m = CloudMonitoringUtils.createMetric(rollup.getVariance());
        rm.setVariance(m);
        m = CloudMonitoringUtils.createMetric(rollup.getMaxValue());
        rm.setMax(m);
        m = CloudMonitoringUtils.createMetric(rollup.getMinValue());
        rm.setMin(m);

        return rm;
    }

    public static RollupMetric buildRollupThriftMetricFromObject(Object data) {
        RollupMetric rm = new RollupMetric();
        telescope.thrift.Metric m = CloudMonitoringUtils.createMetric(data);
        rm.setRawSample(m);
        rm.setNumPoints(1);

        return rm;
    }
}
