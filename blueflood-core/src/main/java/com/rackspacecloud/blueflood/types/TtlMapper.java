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

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.HashMap;
import java.util.Map;

public class TtlMapper {
    // Simple map sufficient as guava map would lock at tenant level
    private Map<Granularity, Map<RollupType, TimeValue>> ttlMap;

    public TtlMapper() {
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
