package com.rackspacecloud.blueflood.utils;

import org.joda.time.Instant;

public class DefaultClockImpl implements Clock {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
