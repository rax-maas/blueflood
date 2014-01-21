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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TtlManager implements TtlProvider {
    private static final Logger log = LoggerFactory.getLogger(TtlManager.class);
    private static final String DEFAULT_TENANT = "*";
    private final LoadingCache<String, GranRollupTypeTtlMap> tenanttTtlMap;

    // these values get used in the absence of a ttl (db lookup failure, etc.).
    static final Map<String, GranRollupTypeTtlMap> SAFETY_TTLS =
            new HashMap<String, GranRollupTypeTtlMap>() {{
                GranRollupTypeTtlMap ttlMap = new GranRollupTypeTtlMap();
                for (Granularity granularity : Granularity.granularities()) {
                    for (RollupType type : RollupType.values()) {
                        ColumnFamily cf = CassandraModel.getColumnFamily(RollupType.classOf(type, granularity), granularity);

                        if (cf instanceof CassandraModel.MetricColumnFamily) {
                            CassandraModel.MetricColumnFamily metricCF = (CassandraModel.MetricColumnFamily) cf;
                            TimeValue ttl = new TimeValue(metricCF.getDefaultTTL().getValue() * 5, metricCF.getDefaultTTL().getUnit());
                            ttlMap.setTtl(granularity, type, ttl);
                        }
                    }
                }
                put(DEFAULT_TENANT, ttlMap);
            }};

    public TtlManager(TimeValue expiration, int cacheConcurrency) {
        CacheLoader<String, GranRollupTypeTtlMap> loader = new CacheLoader<String, GranRollupTypeTtlMap>() {

            @Override
            public GranRollupTypeTtlMap load(final String tenantId) throws Exception {
                // For now return the default values from config file.
                // TODO: Read from database the TTL values
                return null;
            }
        };

        tenanttTtlMap = CacheBuilder.newBuilder()
                .expireAfterWrite(expiration.getValue(), expiration.getUnit())
                .concurrencyLevel(cacheConcurrency)
                .recordStats()
                .build(loader);

    }

    @Override
    public TimeValue getTTL(String tenantId, Granularity gran, RollupType rollupType) throws Exception {
        try {
            return tenanttTtlMap.get(tenantId).getTtl(gran, rollupType);
        } catch (ExecutionException ex) {
            return SAFETY_TTLS.get(DEFAULT_TENANT).getTtl(gran, rollupType);
        }
    }

    @Override
    public void setTTL(String tenantId, Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception {
        GranRollupTypeTtlMap ttlMap = tenanttTtlMap.get(tenantId);

        if (ttlMap == null) {
            ttlMap = new GranRollupTypeTtlMap();
            ttlMap.setTtl(gran, rollupType, ttlValue);
            tenanttTtlMap.put(tenantId, ttlMap);
            // TODO: Flush changes to database.
        }
    }

    public static class GranRollupTypeTtlMap {
        // Simple map sufficient as guava map would lock at tenant level
        private Map<Granularity, Map<RollupType, TimeValue>> ttlMap;

        public GranRollupTypeTtlMap() {
            ttlMap = new HashMap<Granularity, Map<RollupType, TimeValue>>();
        }

        public void setTtl(Granularity gran, RollupType rollupType, TimeValue ttl) {
            Map<RollupType, TimeValue> rollupTypeTtlMap = ttlMap.get(gran);

            if (rollupTypeTtlMap == null) {
               rollupTypeTtlMap = new HashMap<RollupType, TimeValue>();
            }

            rollupTypeTtlMap.put(rollupType, ttl);
        }

        public TimeValue getTtl(Granularity gran, RollupType rollupType) {
            Map<RollupType, TimeValue> rollupTypeTtlMap = ttlMap.get(gran);

            if (rollupTypeTtlMap == null) {
                return null;  // Decide what to do.
            }

            return rollupTypeTtlMap.get(rollupType);
        }
    }
}
