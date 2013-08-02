package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;

import java.util.Set;

public interface OutputSerializer<T> {
    public T transformRollupData(MetricData metricData, Set<String> filterStats) throws SerializationException;
}
