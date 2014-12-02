package com.rackspacecloud.blueflood.service;

import java.util.Collection;

public interface ScheduleContextMBean {
    /**
     * Get the metrics state currently stored by ScheduleContext
     */
    public Collection<String> getMetricsState(int shard, String gran, int slot);
}
