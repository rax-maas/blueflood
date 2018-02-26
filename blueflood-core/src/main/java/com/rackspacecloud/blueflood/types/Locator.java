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

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Locator implements Comparable<Locator> {
    public static final String METRIC_TOKEN_SEPARATOR;
    public static final String METRIC_TOKEN_SEPARATOR_REGEX;
    private static final Logger log = LoggerFactory.getLogger(Locator.class);
    private String stringRep = null;
    private String tenantId = null;
    private String metricName = null;

    static {
        METRIC_TOKEN_SEPARATOR = (Configuration.getInstance().getBooleanProperty(CoreConfig.USE_LEGACY_METRIC_SEPARATOR) ? "," : ".");
        // ugh.
        METRIC_TOKEN_SEPARATOR_REGEX = (Configuration.getInstance().getBooleanProperty(CoreConfig.USE_LEGACY_METRIC_SEPARATOR) ? "," : "\\.");
        if (METRIC_TOKEN_SEPARATOR.equals(",")) {
            log.warn("Deprecation warning! Use of 'USE_LEGACY_METRIC_SEPARATOR' is deprecated and will be removed in v3.0");
        }
    }

    public Locator() {
        // Left empty
    }

    private Locator(String fullyQualifiedMetricName) throws IllegalArgumentException {
        setStringRep(fullyQualifiedMetricName);
    }

    protected void setStringRep(String rep) throws IllegalArgumentException {
        // todo: null check and throw IllegalArgumentException?
        this.stringRep = rep;
        tenantId = this.stringRep.split(METRIC_TOKEN_SEPARATOR_REGEX)[0];
        metricName = this.stringRep.substring(this.stringRep.indexOf(METRIC_TOKEN_SEPARATOR)+1);
    }

    protected boolean isValidDBKey(String dbKey, String delim) {
        return dbKey.contains(delim);
    }

    @Override
    public int hashCode() {
        return stringRep == null ? 0 : stringRep.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj instanceof Locator && obj.hashCode() == this.hashCode();
    }

    public String toString() {
        return stringRep;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public boolean equals(Locator other) {
        return stringRep.equals(other.toString());
    }

    public static Locator createLocatorFromPathComponents(String tenantId, String... parts) throws IllegalArgumentException {
        if(StringUtils.isEmpty(tenantId) || parts == null)
            log.error("'tenantId' is null or empty OR 'parts' is null.");

        return new Locator(tenantId + METRIC_TOKEN_SEPARATOR + StringUtils.join(parts, METRIC_TOKEN_SEPARATOR));
    }

    public static Locator createLocatorFromDbKey(String fullyQualifiedMetricName) throws IllegalArgumentException {
        return new Locator(fullyQualifiedMetricName);
    }

    @Override
    public int compareTo(Locator o) {
        return stringRep.compareTo(o.toString());
    }
}
