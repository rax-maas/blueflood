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

package com.rackspacecloud.blueflood.service;

public enum TtlConfig implements ConfigDefaults {
    // TTL specification for various rollup & data types (specified in days)
    STRING_METRICS_TTL("365"),

    RAW_METRICS_TTL(""),
    BASIC_ROLLUPS_MIN5(""),
    BASIC_ROLLUPS_MIN20(""),
    BASIC_ROLLUPS_MIN60(""),
    BASIC_ROLLUPS_MIN240(""),
    BASIC_ROLLUPS_MIN1440(""),

    SET_ROLLUPS_FULL(""),
    SET_ROLLUPS_MIN5(""),
    SET_ROLLUPS_MIN20(""),
    SET_ROLLUPS_MIN60(""),
    SET_ROLLUPS_MIN240(""),
    SET_ROLLUPS_MIN1440(""),

    GAUGE_ROLLUPS_FULL(""),
    GAUGE_ROLLUPS_MIN5(""),
    GAUGE_ROLLUPS_MIN20(""),
    GAUGE_ROLLUPS_MIN60(""),
    GAUGE_ROLLUPS_MIN240(""),
    GAUGE_ROLLUPS_MIN1440(""),

    TIMER_ROLLUPS_FULL(""),
    TIMER_ROLLUPS_MIN5(""),
    TIMER_ROLLUPS_MIN20(""),
    TIMER_ROLLUPS_MIN60(""),
    TIMER_ROLLUPS_MIN240(""),
    TIMER_ROLLUPS_MIN1440(""),

    TTL_CONFIG_CONST("5"), // 5 days
    ARE_TTLS_FORCED("false");
    static {
        Configuration.getInstance().loadDefaults(TtlConfig.values());
    }

    private String defaultValue;

    private TtlConfig(String value) {
        this.defaultValue = value;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }
}
