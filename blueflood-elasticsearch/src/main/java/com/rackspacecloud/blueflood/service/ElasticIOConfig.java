/**
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

public enum ElasticIOConfig implements ConfigDefaults {
    ELASTICSEARCH_HOSTS("127.0.0.1:9300"),
    ELASTICSEARCH_CLUSTERNAME("elasticsearch"),
    ELASTICSEARCH_INDEX_NAME_WRITE("metric_metadata"),
    ELASTICSEARCH_INDEX_NAME_READ("metric_metadata");

    static {
        Configuration.getInstance().loadDefaults(ElasticIOConfig.values());
    }
    private String defaultValue;
    private ElasticIOConfig(String value) {
        this.defaultValue = value;
    }
    public String getDefaultValue() {
        return defaultValue;
    }
}
