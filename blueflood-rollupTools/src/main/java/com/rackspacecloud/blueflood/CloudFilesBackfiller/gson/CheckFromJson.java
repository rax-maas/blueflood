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

 * Original author: gdusbabek
 */
package com.rackspacecloud.blueflood.CloudFilesBackfiller.gson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class CheckFromJson {
    private static final Collection<String> EMPTY_METRICS = Collections.unmodifiableCollection(new ArrayList<String>());
    
    private String id;
    private long timestamp;
    private String accountId;
    private String tenantId;
    private String entityId;
    private String checkId;
    private String target;
    private String checkType;
    private String monitoringZoneId;
    private String collectorId;
    private Map<String,Map<String, ?>> metrics;

    public String getId() {
        if (id != null)
            return id;
        else
            return String.format("%s.%s.%s.%s", tenantId, entityId, checkId, checkType);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getCheckId() {
        return checkId;
    }

    public String getTarget() {
        if (target != null)
            return target;
        else
            return String.format("%s.%s", checkId, checkType);
    }

    public String getCheckType() {
        return checkType;
    }

    public String getMonitoringZoneId() {
        if (monitoringZoneId != null)
            return monitoringZoneId;
        else
            return "suspected-agent";
    }

    public String getCollectorId() {
        if (collectorId != null)
            return collectorId;
        else
            return "unknown";
    }
    
    public Collection<String> getMetricNames() {
        if (metrics == null)
            return EMPTY_METRICS;
        else
            return metrics.keySet();
    }
    
    public MetricPoint getMetric(String name) {
        return new MetricPoint(metrics.get(name));
    }
    
    public static boolean isValid(CheckFromJson checkFromJson) {
        if (checkFromJson.getId() == null)
            return false;
        if (checkFromJson.getAccountId() == null)
            return false;
        if (checkFromJson.getCheckId() == null)
            return false;
        if (checkFromJson.getCheckType() == null)
            return false;
        if (checkFromJson.getCollectorId() == null)
            return false;
        if (checkFromJson.getEntityId() == null)
            return false;
        if (checkFromJson.getTarget() == null)
            return false;
        if (checkFromJson.getTenantId() == null)
            return false;
        
        return true;
    }

}
