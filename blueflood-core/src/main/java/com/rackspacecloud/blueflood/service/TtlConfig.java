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

    RAW_METRICS_TTL("7"), // 7 days
    BASIC_ROLLUPS_MIN5("30"), // 1 month
    BASIC_ROLLUPS_MIN20("60"), // 2 months
    BASIC_ROLLUPS_MIN60("90"), // 3 months
    BASIC_ROLLUPS_MIN240("180"), // 6 months
    BASIC_ROLLUPS_MIN1440("365"), // 1 year

    HIST_ROLLUPS_MIN5("30"), // 1 month
    HIST_ROLLUPS_MIN20("60"), // 2 months
    HIST_ROLLUPS_MIN60("90"), // 3 months
    HIST_ROLLUPS_MIN240("180"), // 6 months
    HIST_ROLLUPS_MIN1440("365"), // 1 year

    SET_ROLLUPS_FULL("7"), // 7 days
    SET_ROLLUPS_MIN5("30"), // 1 month
    SET_ROLLUPS_MIN20("60"), // 2 months
    SET_ROLLUPS_MIN60("90"), // 3 months
    SET_ROLLUPS_MIN240("180"), // 6 months
    SET_ROLLUPS_MIN1440("365"), // 1 year

    GAUGE_ROLLUPS_FULL("7"), // 7 days
    GAUGE_ROLLUPS_MIN5("30"), // 1 month
    GAUGE_ROLLUPS_MIN20("60"), // 2 months
    GAUGE_ROLLUPS_MIN60("90"), // 3 months
    GAUGE_ROLLUPS_MIN240("180"), // 6 months
    GAUGE_ROLLUPS_MIN1440("365"), // 1 year

    TIMER_ROLLUPS_FULL("7"), // 7 days
    TIMER_ROLLUPS_MIN5("30"), // 1 month
    TIMER_ROLLUPS_MIN20("60"), // 2 months
    TIMER_ROLLUPS_MIN60("90"), // 3 months
    TIMER_ROLLUPS_MIN240("180"), // 6 months
    TIMER_ROLLUPS_MIN1440("365"), // 1 year

    TTL_BIN_LOWER_BOUND0("0"), // 0 seconds
    TTL_BIN_UPPER_BOUND0("5"), // 5 seconds
    TTL_CONFIG_0("2"),  // 2 days

    TTL_BIN_LOWER_BOUND1("5"), // 5 seconds
    TTL_BIN_UPPER_BOUND1("15"), // 15 seconds
    TTL_CONFIG_1("7"), //7 days

    TTL_BIN_LOWER_BOUND2("15"), //15 seconds
    TTL_BIN_UPPER_BOUND2("30"), //30 seconds
    TTL_CONFIG_2("14"), //14 days

    TTL_BIN_LOWER_BOUND3("30"), //30 seconds
    TTL_BIN_UPPER_BOUND3("5"), //5 minutes
    TTL_CONFIG_3("31"), //31 days

    TTL_BIN_LOWER_BOUND4("5"), //5 min
    TTL_BIN_UPPER_BOUND4("20"), //20 min
    TTL_CONFIG_4("62"), //62 days

    TTL_BIN_LOWER_BOUND5("20"), //20 min
    TTL_BIN_UPPER_BOUND5("1"), //1 hr
    TTL_CONFIG_5("93"), //93 days

    TTL_BIN_LOWER_BOUND6("1"), // 1 hour
    TTL_BIN_UPPER_BOUND6("1"), //1 day
    TTL_CONFIG_6("186"), //186 days

    TTL_BIN_LOWER_BOUND7("1"), //1 day
    TTL_BIN_UPPER_BOUND7(Integer.toString(Integer.MAX_VALUE)), //max days
    TTL_CONFIG_7(Integer.toString(Integer.MAX_VALUE)); //max days

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
