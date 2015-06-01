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
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.concurrent.TimeUnit;

public class SafetyTtlProvider implements TenantTtlProvider {
    private final ImmutableTable<Granularity, RollupType, TimeValue> SAFETY_TTLS;
    private final TimeValue STRING_TTLS = Constants.STRING_SAFETY_TTL;
    private final TimeValue CONFIG_TTL = new TimeValue(Configuration.getInstance().getIntegerProperty(TtlConfig.TTL_CONFIG_CONST), TimeUnit.DAYS);
    private final boolean ARE_TTLS_FORCED = Configuration.getInstance().getBooleanProperty(TtlConfig.ARE_TTLS_FORCED);

    private static final SafetyTtlProvider INSTANCE = new SafetyTtlProvider();

    public static SafetyTtlProvider getInstance() {
        return INSTANCE;
    }

    private SafetyTtlProvider() {
        ImmutableTable.Builder<Granularity, RollupType, TimeValue> ttlMapBuilder =
                new ImmutableTable.Builder<Granularity, RollupType, TimeValue>();

        for (Granularity granularity : Granularity.granularities()) {
            for (RollupType type : RollupType.values()) {
                try {
                    ColumnFamily cf = CassandraModel.getColumnFamily(RollupType.classOf(type, granularity), granularity);

                    if (cf instanceof CassandraModel.MetricColumnFamily) {
                        CassandraModel.MetricColumnFamily metricCF = (CassandraModel.MetricColumnFamily) cf;
                        TimeValue ttl = new TimeValue(metricCF.getDefaultTTL().getValue() * 5, metricCF.getDefaultTTL().getUnit());
                        ttlMapBuilder.put(granularity, type, ttl);
                    }
                } catch (IllegalArgumentException ex) {
                    // pass
                }
            }
        }

        this.SAFETY_TTLS = ttlMapBuilder.build();
    }

    @Override
    public TimeValue getTTL(String tenantId, Granularity gran, RollupType rollupType) throws Exception {
        return getSafeTTL(gran, rollupType);
    }
    
    public TimeValue getSafeTTL(Granularity gran, RollupType rollupType) {
        return SAFETY_TTLS.get(gran, rollupType);
    }

    @Override
    public void setTTL(String tenantId, Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception {
        throw new RuntimeException("Not allowed to override safety ttls. They are auto-derived based on granularity.");
    }

    @Override
    public TimeValue getTTLForStrings(String tenantId) throws Exception {
        return STRING_TTLS;
    }

    @Override
    public TimeValue getConfigTTLForIngestion() throws Exception {
       return CONFIG_TTL;
    }

    public long getFinalTTL(String tenantid, Granularity g) {
        long ttl;
        try {
            if (g == Granularity.FULL) {
                if (ARE_TTLS_FORCED) {
                    ttl = getConfigTTLForIngestion().toMillis();
                }
                else {
                    ttl = getTTL(tenantid, g, RollupType.BF_BASIC).toMillis();
                }
            }
            else {
                ttl = getTTL(tenantid, g, RollupType.BF_BASIC).toMillis();
            }
        } catch (Exception ex) {
            ttl = getSafeTTL(
                    g, RollupType.BF_BASIC).toMillis();
        }
        return ttl;
    }
}
