package com.cloudkick.blueflood.outputs.Serializers;

import com.cloudkick.blueflood.outputs.formats.RollupData;

public interface OutputSerializer<T> {
    public T transformRollupData(RollupData rollupData);
}
