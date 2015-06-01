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

package com.rackspacecloud.blueflood.cache;

import com.google.common.collect.ImmutableTable;
import com.rackspacecloud.blueflood.exceptions.ConfigException;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

public class ConfigTtlProvider implements TenantTtlProvider {
    private static final Logger log = LoggerFactory.getLogger(ConfigTtlProvider.class);

    private final ImmutableTable<Granularity, RollupType, TimeValue> ttlMapper;
    private final TimeValue stringTTL;
    private static final ConfigTtlProvider INSTANCE = new ConfigTtlProvider();
    private static final boolean ARE_TTLS_FORCED = Configuration.getInstance().getBooleanProperty(TtlConfig.ARE_TTLS_FORCED);
    private static final TimeValue TTL_CONFIG_FOR_INGESTION = new TimeValue(Configuration.getInstance().getIntegerProperty(TtlConfig.TTL_CONFIG_CONST), TimeUnit.DAYS);

    public static ConfigTtlProvider getInstance() {
        return INSTANCE;
    }

    private ConfigTtlProvider() {
        final Configuration config = Configuration.getInstance();

        // String rollups
        stringTTL = new TimeValue(config.getIntegerProperty(TtlConfig.STRING_METRICS_TTL), TimeUnit.DAYS);
        ImmutableTable.Builder<Granularity, RollupType, TimeValue> ttlMapBuilder =
                new ImmutableTable.Builder<Granularity, RollupType, TimeValue>();

        // Basic rollups
        ttlMapBuilder.put(Granularity.FULL, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.RAW_METRICS_TTL), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_5, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.BASIC_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_20, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.BASIC_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_60, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.BASIC_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_240, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.BASIC_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_1440, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(TtlConfig.BASIC_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Histogram rollups

        ttlMapBuilder.put(Granularity.MIN_5, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(TtlConfig.HIST_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_20, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(TtlConfig.HIST_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_60, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(TtlConfig.HIST_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_240, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(TtlConfig.HIST_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_1440, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(TtlConfig.HIST_ROLLUPS_MIN1440), TimeUnit.DAYS));

        /* Pre-aggregated rollups */

        // Set rollups
        ttlMapBuilder.put(Granularity.FULL, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_5, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_20, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_60, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_240, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_1440, RollupType.SET,
                new TimeValue(config.getIntegerProperty(TtlConfig.SET_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Timer rollups
        ttlMapBuilder.put(Granularity.FULL, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_5, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_20, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_60, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_240, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_1440, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(TtlConfig.TIMER_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Gauge rollups
        ttlMapBuilder.put(Granularity.FULL, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_5, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_20, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_60, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_240, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapBuilder.put(Granularity.MIN_1440, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(TtlConfig.GAUGE_ROLLUPS_MIN1440), TimeUnit.DAYS));
        this.ttlMapper = ttlMapBuilder.build();
    }

    @Override
    public TimeValue getTTL(String tenantId, Granularity gran, RollupType rollupType) throws Exception {
        final TimeValue ttl = ttlMapper.get(gran, rollupType);

        if (ttl == null) {
            log.warn("No valid TTL entry for granularity: " + gran + ", rollup type: " + rollupType.name()
                    + " in config. Resorting to safe TTL values.");
            throw new ConfigException("No TTL config found for granularity: " + gran
                    + ", rollup type: " + rollupType.name());
        }

        return ttl;
    }

    @Override
    public void setTTL(String tenantId, Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception {
        throw new RuntimeException("Not allowed to override TTL values specified in configs.");
    }

    @Override
    public TimeValue getTTLForStrings(String tenantId) throws Exception {
        return stringTTL;
    }

    @Override
    public TimeValue getConfigTTLForIngestion() {
        return TTL_CONFIG_FOR_INGESTION;
    }

    public boolean areTTLsForced() {
        return ARE_TTLS_FORCED;
    }
}
