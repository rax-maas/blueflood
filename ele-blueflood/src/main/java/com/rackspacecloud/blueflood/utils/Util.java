package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import telescope.thrift.Metric;

public class Util {
    public static Metric createMetric(Object o) {
        Metric m = new Metric();
        if (o instanceof Double) {
            m.setValueDbl((Double) o);
            m.setMetricType((byte)MetricHelper.Type.DOUBLE);
        } else if (o instanceof Long) {
            m.setValueI64((Long)o);
            m.setMetricType((byte)MetricHelper.Type.UINT64);
        } else if (o instanceof Integer) {
            m.setValueI32((Integer)o);
            m.setMetricType((byte)MetricHelper.Type.INT32);
        } else if (o instanceof AbstractRollupStat) {
            AbstractRollupStat stat = (AbstractRollupStat) o;
            if (stat.isFloatingPoint()) {
                m.setValueDbl(stat.toDouble());
                m.setMetricType((byte)MetricHelper.Type.DOUBLE);
            } else {
                m.setValueI64(stat.toLong());
                m.setMetricType((byte)MetricHelper.Type.INT64);
            }
        } else if (o instanceof String) {
            m.setValueStr((String)o);
            m.setMetricType((byte)MetricHelper.Type.STRING);
        } else if (o instanceof Boolean) {
            m.setValueBool((Boolean)o);
            m.setMetricType((byte)MetricHelper.Type.BOOLEAN);
        }
        else throw new RuntimeException("Unexpected type for rollup: " + o.getClass().getName());
        return m;
    }
}
