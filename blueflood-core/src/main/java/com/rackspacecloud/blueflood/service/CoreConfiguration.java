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
 * This is the configuration singleton for blueflood-core.
 * It has default values for all blueflood-core settings.
 * resources/blueflood.properties contains default values of all settings
 */
public class CoreConfiguration extends Configuration {
    private static String defaultPropFileName = "blueflood.properties";
    private static final CoreConfiguration INSTANCE = new CoreConfiguration();
    private CoreConfiguration() {
        super(defaultPropFileName);
    }
    public static CoreConfiguration getInstance() {
        return INSTANCE;
    }
}
