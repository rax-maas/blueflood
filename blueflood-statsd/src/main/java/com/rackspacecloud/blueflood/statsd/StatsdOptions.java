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

package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.service.Configuration;

public class StatsdOptions {
    private final boolean legacyNamespace;
 
    private final String globalPrefix;
    private final String globalSuffix;
    private final String prefixStats;

    private final String prefixCounter;
    private final String prefixGauge;
    private final String prefixSet;
    private final String prefixTimer;
    
    private final String internalTenantId;
    
    public StatsdOptions(Configuration conf) {
        legacyNamespace = conf.getBooleanProperty(StatsdConfig.STATSD_LEGACY_NAMESPACE);
        if (legacyNamespace) {
            // ignore configured values and use these defaults.
            prefixCounter = "stats_counts";
            prefixGauge = StatsdConfig.STATSD_PREFIX_GAUGE.getDefaultValue();
            prefixSet = StatsdConfig.STATSD_PREFIX_SET.getDefaultValue();
            prefixTimer = StatsdConfig.STATSD_PREFIX_TIMER.getDefaultValue();
            globalPrefix = StatsdConfig.STATSD_GLOBAL_PREFIX.getDefaultValue(); // except for internal gauges. "numStats"
            
        } else {
            prefixCounter = conf.getStringProperty(StatsdConfig.STATSD_PREFIX_COUNTER);
            prefixGauge = conf.getStringProperty(StatsdConfig.STATSD_PREFIX_GAUGE);
            prefixSet = conf.getStringProperty(StatsdConfig.STATSD_PREFIX_SET);
            prefixTimer = conf.getStringProperty(StatsdConfig.STATSD_PREFIX_TIMER);
            globalPrefix = conf.getStringProperty(StatsdConfig.STATSD_GLOBAL_PREFIX);
        }
        
        prefixStats = conf.getStringProperty(StatsdConfig.STATSD_PREFIX_INTERNAL);
        globalSuffix = conf.getStringProperty(StatsdConfig.STATSD_GLOBAL_SUFFIX);
        internalTenantId = conf.getStringProperty(StatsdConfig.INTERNAL_TENTANT_ID);
    }


    public boolean isLegacyNamespace() {
        return legacyNamespace;
    }

    public String getGlobalPrefix() {
        return globalPrefix;
    }

    public String getGlobalSuffix() {
        return globalSuffix;
    }

    public String getPrefixStats() {
        return prefixStats;
    }

    public String getPrefixCounter() {
        return prefixCounter;
    }

    public String getPrefixGauge() {
        return prefixGauge;
    }

    public String getPrefixSet() {
        return prefixSet;
    }

    public String getPrefixTimer() {
        return prefixTimer;
    }
    
    public String getInternalTenantId() { return internalTenantId; }
    
    public boolean hasGlobalPrefix() { return globalPrefix != null && globalPrefix.length() > 0; }
    public boolean hasGlobalSuffix() { return globalSuffix != null && globalSuffix.length() > 0; }
}
