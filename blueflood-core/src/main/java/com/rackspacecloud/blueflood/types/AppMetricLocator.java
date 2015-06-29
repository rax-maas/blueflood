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

package com.rackspacecloud.blueflood.types;

public class AppMetricLocator extends Locator {
    private static final String incomingDelim = ".";
    private static final String persistDelim = ",";
    private static final int MAX_FIELDS = 4;

    private String service;
    private String host;
    private String instance;
    private String metricName;

    public static AppMetricLocator createFromServicePrimitives(String service, String host, String instance,
                                                               String metricName) {
        return new AppMetricLocator(service, host, instance, metricName);
    }

    public static AppMetricLocator createFromDBKey(String locator) {
        return new AppMetricLocator(locator);
    }

    private AppMetricLocator(String locator) {
        if (!isValidDBKey(locator, persistDelim)) {
            throw new IllegalArgumentException("Expected delimiter " + "'" + persistDelim + "' " + "but got " +
                    locator);
        }

        String[] tokens = locator.split(persistDelim);
        this.service = tokens[0];
        this.host = tokens[1];
        this.instance = tokens[2];
        this.metricName = tokens[3];
        setStringRep(this.buildStringRep());
    }

    private AppMetricLocator(String service, String host, String instance, String metricName) {
        this.service = service;
        this.host = host;
        this.instance = instance;
        this.metricName = metricName;
        setStringRep(this.buildStringRep());
    }

    public String getService() {
        return service;
    }

    public String getHost() {
        return host;
    }

    public String getInstanceId() {
        return instance;
    }

    public String getMetricName() {
        return metricName;
    }

    private String buildStringRep() {
        return String.format("%s,%s,%s,%s", this.service, this.host, this.instance, this.metricName);
    }

    public boolean equals(Locator other) {
        return other.toString().equals(toString());
    }

    public String getDBKey() {
        return toString();
    }
}