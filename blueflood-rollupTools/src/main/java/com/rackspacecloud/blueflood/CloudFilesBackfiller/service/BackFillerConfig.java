package com.rackspacecloud.blueflood.CloudFilesBackfiller.service;

import com.rackspacecloud.blueflood.service.ConfigDefaults;
import com.rackspacecloud.blueflood.service.Configuration;

public enum BackFillerConfig implements ConfigDefaults {
    CLOUDFILES_USER("username"),
    CLOUDFILES_KEY("password"),
    CLOUDFILES_PROVIDER("cloudfiles-us"),
    CLOUDFILES_ZONE("IAD"),
    CLOUDFILES_CONTAINER("metric-data"),

    DOWNLOAD_DIR("/tmp/metrics_gzipped_prod"),
    BATCH_SIZE("5"),

    ROLLUP_DIR("/tmp/metrics_rollup_prod"),

    // TODO: In order to account for back pressure, we will need to give a buffer window around the replay period * I think *
    REPLAY_PERIOD_START("1400122800000"),
    REPLAY_PERIOD_STOP("1400612400000"),

    NUMBER_OF_BUFFERRED_SLOTS("3"),

    ROLLUP_THREADS("50"),

    BATCH_WRITER_THREADS("5"),

    SHARDS_TO_BACKFILL("69,70,71,119,72,73,74,75,88,89,90,91");

    static {
        Configuration.getInstance().loadDefaults(BackFillerConfig.values());
    }
    private String defaultValue;
    private BackFillerConfig(String value) {
        this.defaultValue = value;
    }
    @Override
    public String getDefaultValue() {
        return defaultValue;
    }
}
