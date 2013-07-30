package com.cloudkick.blueflood.outputs.Serializers;

import com.cloudkick.blueflood.outputs.formats.RollupData;
import com.cloudkick.blueflood.types.Points;
import com.cloudkick.blueflood.types.Rollup;
import com.cloudkick.blueflood.utils.Util;
import telescope.thrift.RollupMetric;
import telescope.thrift.RollupMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultThriftOutputSerializer implements OutputSerializer<RollupMetrics> {

    @Override
    public RollupMetrics transformRollupData(RollupData rollupData) {
        final Points points = rollupData.getData();
        return new RollupMetrics(transformPoints(points), rollupData.getUnit());
    }

    public List<RollupMetric> transformPoints(Points points) {
        final List<RollupMetric> rollupMetricsList = new ArrayList<RollupMetric>();

        final Set<Map.Entry<Long, Points.Point>> data = points.getPoints().entrySet();
        for (Map.Entry<Long, Points.Point> item : data) {
            final RollupMetric rollupMetric = transforPointToRollupMetric(item.getValue());
            rollupMetricsList.add(rollupMetric);
        }

        return rollupMetricsList;
    }

    public RollupMetric transforPointToRollupMetric(Points.Point point) {
        RollupMetric rollupMetric = new RollupMetric();
        rollupMetric.setTimestamp(point.getTimestamp());

        if (point.getData() instanceof Rollup) {
            rollupMetric = buildRollupThriftMetricFromRollup(rollupMetric, (Rollup) point.getData());
        } else {
            rollupMetric = buildRollupThriftMetricFromObject(rollupMetric, point.getData());
        }

        return rollupMetric;
    }

    public static RollupMetric buildRollupThriftMetricFromRollup(RollupMetric rm, Rollup rollup) {
        rm.setNumPoints(rollup.getCount());

        telescope.thrift.Metric m = Util.createMetric(rollup.getAverage());
        rm.setAverage(m);
        m = Util.createMetric(rollup.getVariance());
        rm.setVariance(m);
        m = Util.createMetric(rollup.getMaxValue());
        rm.setMax(m);
        m = Util.createMetric(rollup.getMinValue());
        rm.setMin(m);

        return rm;
    }

    public static RollupMetric buildRollupThriftMetricFromObject(RollupMetric rm, Object data) {
        telescope.thrift.Metric m = Util.createMetric(data);
        rm.setRawSample(m);
        rm.setNumPoints(1);

        return rm;
    }
}
