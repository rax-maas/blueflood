
/*
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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

    NUMBER_OF_BUFFERED_SLOTS("3"),

    ROLLUP_THREADS("50"),

    BATCH_WRITER_THREADS("5"),

    SHARDS("69,70,71,119,72,73,74,75,88,89,90,91");

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
