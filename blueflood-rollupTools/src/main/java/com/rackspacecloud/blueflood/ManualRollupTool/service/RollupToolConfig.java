/**
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

package com.rackspacecloud.blueflood.ManualRollupTool.service;

import com.rackspacecloud.blueflood.service.ConfigDefaults;
import com.rackspacecloud.blueflood.service.Configuration;

public enum RollupToolConfig implements ConfigDefaults {
    START_MILLIS("1392811200000"), // Human time (GMT): Wed, 19 Feb 2014 12:00:00 GMT
    STOP_MILLIS("1392984000000"),  //                   Fri, 21 Feb 2014 12:00:00 GMT
    MAX_REROLL_THREADS("2"),

    METRICS_5M_ENABLED("true"),
    METRICS_20M_ENABLED("false"),
    METRICS_60M_ENABLED("false"),
    METRICS_240M_ENABLED("false"),
    METRICS_1440M_ENABLED("false"),

    SHARDS_TO_MANUALLY_ROLLUP("69,70,71,119,72,73,74,75,88,89,90,91");

    static {
        Configuration.getInstance().loadDefaults(RollupToolConfig.values());
    }
    private String defaultValue;
    private RollupToolConfig(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }
}
