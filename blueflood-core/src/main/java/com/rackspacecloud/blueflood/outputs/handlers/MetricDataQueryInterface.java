package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.types.Resolution;

public interface MetricDataQueryInterface<T> {
    public T GetDataByPoints(String tenantId, String metric, long from, long to, int points) throws Exception;

    public T GetDataByResolution(String tenantId, String metric, long from, long to, Resolution resolution) throws Exception;
}
