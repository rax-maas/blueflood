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

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.types.TtlMapper;
import com.rackspacecloud.blueflood.utils.TimeValue;

public class SafetyTtlProvider implements SimpleTtlProvider {
    private static final TtlMapper SAFETY_TTLS = new TtlMapper();
    private static final TimeValue STRING_TTLS = Constants.STRING_SAFETY_TTL;

    static {
        for (Granularity granularity : Granularity.granularities()) {
            for (RollupType type : RollupType.values()) {
                ColumnFamily cf = CassandraModel.getColumnFamily(RollupType.classOf(type, granularity), granularity);

                if (cf instanceof CassandraModel.MetricColumnFamily) {
                    CassandraModel.MetricColumnFamily metricCF = (CassandraModel.MetricColumnFamily) cf;
                    TimeValue ttl = new TimeValue(metricCF.getDefaultTTL().getValue() * 5, metricCF.getDefaultTTL().getUnit());
                    SAFETY_TTLS.setTtl(granularity, type, ttl);
                }
            }
        }
    }

    @Override
    public TimeValue getTTL(Granularity gran, RollupType rollupType) throws Exception {
        return SAFETY_TTLS.getTtl(gran, rollupType);
    }

    @Override
    public void setTTL(Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception {
        throw new RuntimeException("Not allowed to override safety ttls. They are auto-derived based on granularity.");
    }

    @Override
    public TimeValue getTTLForStrings() throws Exception {
        return STRING_TTLS;
    }
}
