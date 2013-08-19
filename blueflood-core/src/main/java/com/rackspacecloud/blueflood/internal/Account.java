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

package com.rackspacecloud.blueflood.internal;

import com.rackspacecloud.blueflood.utils.TimeValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/** expose more bits as they are needed. */
public class Account {
    private static final Gson gson;
    
    private String id;
    private String external_id;
    private Map<String, Integer> metrics_ttl;
    
    static {
        gson = new GsonBuilder().serializeNulls().create();
    }
    
    public static Account fromJSON(String json) {
        return gson.fromJson(json, Account.class);
    }
    
    public TimeValue getMetricTtl(String resolution) {
        return new TimeValue(metrics_ttl.get(resolution), TimeUnit.HOURS);
    }
    
    public String getId() { return id; };
    
    public String getTenantId() { return external_id; };
     
}
