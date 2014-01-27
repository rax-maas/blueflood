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

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.types.TtlMapper;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ConfigTtlProvider implements SimpleTtlProvider {
    private static final Logger log = LoggerFactory.getLogger(ConfigTtlProvider.class);

    private static final Configuration config = Configuration.getInstance();
    private static final TtlMapper ttlMapper;
    private static final TimeValue stringTTL;
    private static final SafetyTtlProvider fallback = new SafetyTtlProvider();

    static {
        ttlMapper = new TtlMapper();

        // String rollups
        stringTTL = new TimeValue(config.getIntegerProperty(CoreConfig.STRING_METRICS_TTL), TimeUnit.DAYS);

        // Basic rollups
        ttlMapper.setTtl(Granularity.FULL, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.RAW_METRICS_TTL), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_5, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.BASIC_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_20, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.BASIC_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_60, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.BASIC_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_240, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.BASIC_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_1440, RollupType.BF_BASIC,
                new TimeValue(config.getIntegerProperty(CoreConfig.BASIC_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Histogram rollups

        ttlMapper.setTtl(Granularity.MIN_5, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(CoreConfig.HIST_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_20, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(CoreConfig.HIST_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_60, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(CoreConfig.HIST_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_240, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(CoreConfig.HIST_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_1440, RollupType.BF_HISTOGRAMS,
                new TimeValue(config.getIntegerProperty(CoreConfig.HIST_ROLLUPS_MIN1440), TimeUnit.DAYS));

        /* Pre-aggregated rollups */

        // Set rollups
        ttlMapper.setTtl(Granularity.FULL, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_5, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_20, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_60, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_240, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_1440, RollupType.SET,
                new TimeValue(config.getIntegerProperty(CoreConfig.SET_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Timer rollups
        ttlMapper.setTtl(Granularity.FULL, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_5, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_20, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_60, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_240, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_1440, RollupType.TIMER,
                new TimeValue(config.getIntegerProperty(CoreConfig.TIMER_ROLLUPS_MIN1440), TimeUnit.DAYS));

        // Gauge rollups
        ttlMapper.setTtl(Granularity.FULL, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_FULL), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_5, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_MIN5), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_20, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_MIN20), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_60, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_MIN60), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_240, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_MIN240), TimeUnit.DAYS));
        ttlMapper.setTtl(Granularity.MIN_1440, RollupType.GAUGE,
                new TimeValue(config.getIntegerProperty(CoreConfig.GAUGE_ROLLUPS_MIN1440), TimeUnit.DAYS));
    }

    @Override
    public TimeValue getTTL(Granularity gran, RollupType rollupType) throws Exception {
        final TimeValue ttl = ttlMapper.getTtl(gran, rollupType);

        if (ttl == null) {
            log.warn("No valid TTL entry for granularity: " + gran + ", rollup type: " + rollupType.name()
                    + " in config. Resorting to safe TTL values.");
            return fallback.getTTL(gran, rollupType);
        }

        return ttl;
    }

    @Override
    public void setTTL(Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception {
        throw new RuntimeException("Not allowed to override TTL values specified in configs.");
    }

    @Override
    public TimeValue getTTLForStrings() throws Exception {
        return stringTTL;
    }
}
