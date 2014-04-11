package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.health.HealthCheck;

public class NotDOAHealthCheck extends HealthCheck {

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
