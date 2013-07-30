package com.cloudkick.blueflood.outputs.handlers;

import telescope.thrift.Resolution;

public interface RollupsQueryInterface<T> {
    public T GetDataByPoints(String accountId, String metric, long from, long to, int points) throws Exception;

    public T GetDataByResolution(String accountId, String metric, long from, long to, Resolution resolution) throws Exception;
}
