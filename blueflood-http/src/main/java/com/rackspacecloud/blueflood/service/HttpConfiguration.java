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
 * This is the configuration singleton for blueflood-http.
 * It has default values for all blueflood-http settings.
 * resources/blueflood-http.properties contains default values of all settings
 */
public class HttpConfiguration extends Configuration {
    private static final String defaultPropFileName = "blueflood-http.properties";
    private static final HttpConfiguration INSTANCE = new HttpConfiguration();
    private HttpConfiguration() {
        super(defaultPropFileName);
    }
    public static HttpConfiguration getInstance() {
        return INSTANCE;
    }
}
