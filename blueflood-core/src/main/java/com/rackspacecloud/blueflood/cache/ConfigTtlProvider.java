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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableTable;
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

    ConfigTtlProvider() {
        final Configuration config = Configuration.getInstance();

        // String rollups
        TimeValue stringTTL = null;
        try {
            int value = config.getIntegerProperty(TtlConfig.STRING_METRICS_TTL);
            if (value > 0) {
                stringTTL = new TimeValue(config.getIntegerProperty(TtlConfig.STRING_METRICS_TTL), TimeUnit.DAYS);
            }
        } catch (NumberFormatException ex) {
            log.debug("No valid String TTL in config.");
        }
        this.stringTTL = stringTTL;

        ImmutableTable.Builder<Granularity, RollupType, TimeValue> ttlMapBuilder =
                new ImmutableTable.Builder<Granularity, RollupType, TimeValue>();

        // Basic rollups
        put(ttlMapBuilder, config, Granularity.FULL, RollupType.BF_BASIC, TtlConfig.RAW_METRICS_TTL);
        put(ttlMapBuilder, config, Granularity.MIN_5, RollupType.BF_BASIC, TtlConfig.BASIC_ROLLUPS_MIN5);
        put(ttlMapBuilder, config, Granularity.MIN_20, RollupType.BF_BASIC, TtlConfig.BASIC_ROLLUPS_MIN20);
        put(ttlMapBuilder, config, Granularity.MIN_60, RollupType.BF_BASIC, TtlConfig.BASIC_ROLLUPS_MIN60);
        put(ttlMapBuilder, config, Granularity.MIN_240, RollupType.BF_BASIC, TtlConfig.BASIC_ROLLUPS_MIN240);
        put(ttlMapBuilder, config, Granularity.MIN_1440, RollupType.BF_BASIC, TtlConfig.BASIC_ROLLUPS_MIN1440);

        /* Pre-aggregated rollups */

        // Set rollups
        put(ttlMapBuilder, config, Granularity.FULL, RollupType.SET, TtlConfig.SET_ROLLUPS_FULL);
        put(ttlMapBuilder, config, Granularity.MIN_5, RollupType.SET, TtlConfig.SET_ROLLUPS_MIN5);
        put(ttlMapBuilder, config, Granularity.MIN_20, RollupType.SET, TtlConfig.SET_ROLLUPS_MIN20);
        put(ttlMapBuilder, config, Granularity.MIN_60, RollupType.SET, TtlConfig.SET_ROLLUPS_MIN60);
        put(ttlMapBuilder, config, Granularity.MIN_240, RollupType.SET, TtlConfig.SET_ROLLUPS_MIN240);
        put(ttlMapBuilder, config, Granularity.MIN_1440, RollupType.SET, TtlConfig.SET_ROLLUPS_MIN1440);
        // Timer rollups
        put(ttlMapBuilder, config, Granularity.FULL, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_FULL);
        put(ttlMapBuilder, config, Granularity.MIN_5, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_MIN5);
        put(ttlMapBuilder, config, Granularity.MIN_20, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_MIN20);
        put(ttlMapBuilder, config, Granularity.MIN_60, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_MIN60);
        put(ttlMapBuilder, config, Granularity.MIN_240, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_MIN240);
        put(ttlMapBuilder, config, Granularity.MIN_1440, RollupType.TIMER, TtlConfig.TIMER_ROLLUPS_MIN1440);
        // Gauge rollups
        put(ttlMapBuilder, config, Granularity.FULL, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_FULL);
        put(ttlMapBuilder, config, Granularity.MIN_5, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_MIN5);
        put(ttlMapBuilder, config, Granularity.MIN_20, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_MIN20);
        put(ttlMapBuilder, config, Granularity.MIN_60, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_MIN60);
        put(ttlMapBuilder, config, Granularity.MIN_240, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_MIN240);
        put(ttlMapBuilder, config, Granularity.MIN_1440, RollupType.GAUGE, TtlConfig.GAUGE_ROLLUPS_MIN1440);
        this.ttlMapper = ttlMapBuilder.build();
    }

    /**
     * Helper function to build the ttl mapping. Only insert to the mapping if the value is a valid date.
     * @param ttlMapBuilder
     * @param config
     * @param gran
     * @param rollupType
     * @param configKey
     * @return true if the insertion is successful, false otherwise.
     */
    private boolean put(
        ImmutableTable.Builder<Granularity, RollupType, TimeValue> ttlMapBuilder,
        Configuration config,
        Granularity gran,
        RollupType rollupType,
        TtlConfig configKey) {
        int value;
        try {
            value = config.getIntegerProperty(configKey);
            if (value < 0) return false;
        } catch (NumberFormatException ex) {
            log.trace(String.format("No valid TTL config set for granularity: %s, rollup type: %s",
                    gran.name(), rollupType.name()), ex);
            return false;
        }
        ttlMapBuilder.put(gran, rollupType, new TimeValue(value, TimeUnit.DAYS));
        return true;
    }

    @Override
    public Optional<TimeValue> getTTL(String tenantId, Granularity gran, RollupType rollupType) {
        final TimeValue ttl = ttlMapper.get(gran, rollupType);

        if (ttl == null) {
            log.trace("No valid TTL entry for granularity: {}, rollup type: {}" +
                      " in config. Resorting to safe TTL values.",
                       gran.name(), rollupType.name());
            return Optional.absent();
        }

        return Optional.of(ttl);
    }

    @Override
    public Optional<TimeValue> getTTLForStrings(String tenantId) {
        return Optional.of(stringTTL);
    }

    public TimeValue getConfigTTLForIngestion() {
        return TTL_CONFIG_FOR_INGESTION;
    }

    public boolean areTTLsForced() {
        return ARE_TTLS_FORCED;
    }
}
