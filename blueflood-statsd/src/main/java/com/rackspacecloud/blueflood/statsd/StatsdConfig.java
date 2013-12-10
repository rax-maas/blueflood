package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.service.ConfigDefaults;
import com.rackspacecloud.blueflood.service.Configuration;

public enum StatsdConfig implements ConfigDefaults {
    GRAPHITE_INGEST_ADDRESS("127.0.0.1"),
    GRAPHITE_INGEST_PORT("8126")
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
