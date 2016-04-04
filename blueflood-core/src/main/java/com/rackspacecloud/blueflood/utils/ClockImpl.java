package com.rackspacecloud.blueflood.utils;

import org.joda.time.Instant;

public class ClockImpl implements Clock {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
