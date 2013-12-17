package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.service.ConfigDefaults;
import com.rackspacecloud.blueflood.service.Configuration;

public enum StatsdConfig implements ConfigDefaults {
    GRAPHITE_INGEST_ADDRESS("127.0.0.1"),
    GRAPHITE_INGEST_PORT("8126"),
    
    STATSD_LEGACY_NAMESPACE("true"),
    STATSD_GLOBAL_PREFIX("stats"),
    STATSD_PREFIX_TIMER("timers"),
    STATSD_PREFIX_GAUGE("gauges"),
    STATSD_PREFIX_SET("sets"),
    STATSD_PREFIX_COUNTER("counters"),
    STATSD_GLOBAL_SUFFIX("".intern()),
    STATSD_PREFIX_INTERNAL("statsd"),
    INTERNAL_TENTANT_ID("META_STATS_TENANT_ID")
    ;

    static {
        Configuration.getInstance().loadDefaults(StatsdConfig.values());
    }
    
    private String defaultValue;
    private StatsdConfig(String value) {
        this.defaultValue = value;
    }
    
    @Override
    public String getDefaultValue() { return defaultValue; }
}
