package com.cloudkick.blueflood.outputs.serializers;

import com.cloudkick.blueflood.outputs.formats.MetricData;

public interface OutputSerializer<T> {
    public T transformRollupData(MetricData metricData);
}
