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

public enum CloudfilesConfig implements ConfigDefaults {
    CLOUDFILES_USERNAME(""),
    CLOUDFILES_API_KEY(""),
    CLOUDFILES_CONTAINER_FORMAT("'blueflood'-yyyy-MM"), // Java SimpleDateFormat
    CLOUDFILES_ZONE("IAD"),
    CLOUDFILES_MAX_BUFFER_AGE("3600000"), // 1000*60*60 = 60 minutes
    CLOUDFILES_MAX_BUFFER_SIZE("104857600"), // 1024*1024*100 = 100MB
    CLOUDFILES_BUFFER_DIR("./CLOUDFILES_BUFFER"),
    CLOUDFILES_HOST_UNIQUE_IDENTIFIER("bf-host");

    static {
        Configuration.getInstance().loadDefaults(CloudfilesConfig.values());
    }

    private String defaultValue;

    private CloudfilesConfig(String value) {
        this.defaultValue = value;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}
