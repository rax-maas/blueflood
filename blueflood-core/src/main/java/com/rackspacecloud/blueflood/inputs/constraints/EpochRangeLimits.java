package com.rackspacecloud.blueflood.inputs.constraints;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;

/**
 * Constants to set limits for allowed epoch ranges.
 */
public enum EpochRangeLimits {

    BEFORE_CURRENT_TIME_MS(Configuration.getInstance().getLongProperty(CoreConfig.BEFORE_CURRENT_COLLECTIONTIME_MS)),
    AFTER_CURRENT_TIME_MS(Configuration.getInstance().getLongProperty(CoreConfig.AFTER_CURRENT_COLLECTIONTIME_MS));

    private long value;

    EpochRangeLimits(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }
}


