package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.outputs.formats.MetricData;

public interface OutputSerializer<T> {
    public T transformRollupData(MetricData metricData);
}
