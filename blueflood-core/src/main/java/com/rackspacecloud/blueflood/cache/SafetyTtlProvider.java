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

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableTable;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.CassandraModel.MetricColumnFamily;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;
import java.util.concurrent.TimeUnit;

/**
 * Implementation that provides the safe lower bound for the metric TTLs.
 *
 * Even though the methods of this class return {@link Optional}, they always return a present value.
 */
public class SafetyTtlProvider implements TenantTtlProvider {
    private static final TimeValue DAY = new TimeValue(1, TimeUnit.DAYS);
    private final ImmutableTable<Granularity, RollupType, TimeValue> SAFETY_TTLS;
    private final TimeValue STRING_TTL = Constants.STRING_SAFETY_TTL;

    private static final SafetyTtlProvider INSTANCE = new SafetyTtlProvider();

    static SafetyTtlProvider getInstance() {
        return INSTANCE;
    }

    SafetyTtlProvider() {
        ImmutableTable.Builder<Granularity, RollupType, TimeValue> ttlMapBuilder =
                new ImmutableTable.Builder<Granularity, RollupType, TimeValue>();

        for (Granularity granularity : Granularity.granularities()) {
            for (RollupType type : RollupType.values()) {
                if (type == RollupType.NOT_A_ROLLUP) {
                    continue;
                }
                MetricColumnFamily metricCF = CassandraModel.getColumnFamily(RollupType.classOf(type, granularity), granularity);
                TimeValue ttl = new TimeValue(metricCF.getDefaultTTL().getValue(), metricCF.getDefaultTTL().getUnit());
                ttlMapBuilder.put(granularity, type, ttl);
            }
        }

        this.SAFETY_TTLS = ttlMapBuilder.build();
    }

    @Override
    public Optional<TimeValue> getTTL(String tenantId, Granularity gran, RollupType rollupType) {
        return getSafeTTL(gran, rollupType);
    }
    
    public Optional<TimeValue> getSafeTTL(Granularity gran, RollupType rollupType) {
        return Optional.of(MoreObjects.firstNonNull(SAFETY_TTLS.get(gran, rollupType), DAY));
    }

    @Override
    public Optional<TimeValue> getTTLForStrings(String tenantId) {
        return Optional.of(STRING_TTL);
    }
}
