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
public enum HttpConfig implements ConfigDefaults {
    // blueflood can receive metric over HTTP
    HTTP_INGESTION_PORT("19000"),

    // interface to which the ingestion server will bind
    HTTP_INGESTION_HOST("localhost"),

    // blueflood can output metrics over HTTP
    HTTP_METRIC_DATA_QUERY_PORT("20000"),

    // interface to which the query server will bind
    HTTP_QUERY_HOST("localhost"),

    // Maximum number of metrics allowed to be fetched per batch query
    MAX_METRICS_PER_BATCH_QUERY("100"),

    // Maximum number of ACCEPT threads for HTTP output
    MAX_READ_ACCEPT_THREADS("10"),

    /*
      Maximum number of WORKER threads for HTTP output (is included in connections calculations)
      CoreConfig.ES_UNIT_THREADS should also be adjusted corresponding to the changes in this config
      if CoreConfig,.USE_ES_FOR_UNITS is set to true, so that the ES_UNIT threads do not become a
      bottleneck for the netty threads
     */
    MAX_READ_WORKER_THREADS("50"),

    // Maximum number of ACCEPT threads for HTTP input server
    MAX_WRITE_ACCEPT_THREADS("10"),

    // Maximum number of WORKER threads for HTTP output (must be included in connections calculations)
    MAX_WRITE_WORKER_THREADS("50"),

    // Maximum number of batch requests that can be queued
    MAX_BATCH_READ_REQUESTS_TO_QUEUE("10"),

    // Maximum number of threads in type and unit processor threadpool
    HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS("10");

    static {
        Configuration.getInstance().loadDefaults(HttpConfig.values());
    }
    private String defaultValue;
    private HttpConfig(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }
}
