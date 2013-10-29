/*
 * Copyright 2013 Rackspace
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

/**
 * Default config values for blueflood-http. Also to be used for getting config key names.
 */
public enum HttpConfigDefaults implements ConfigDefaults {
    // blueflood can receive metric over HTTP
    HTTP_INGESTION_PORT("19000"),

    // blueflood can output metrics over HTTP
    HTTP_METRIC_DATA_QUERY_PORT("20000");

    static {
        Configuration.getInstance().loadDefaults(HttpConfigDefaults.values());
    }
    private String defaultValue;
    private HttpConfigDefaults(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }
}
