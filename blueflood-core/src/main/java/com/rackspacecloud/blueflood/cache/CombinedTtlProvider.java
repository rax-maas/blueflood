/*
 * Copyright 2016 Rackspace
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;

/**
 * TTL Provider which obtains the TTL from the primary or secondary provider.
 */
public class CombinedTtlProvider implements TenantTtlProvider {
    private final ConfigTtlProvider primary;
    private final SafetyTtlProvider safety;

    private static final CombinedTtlProvider INSTANCE = new CombinedTtlProvider(
            ConfigTtlProvider.getInstance(),
            SafetyTtlProvider.getInstance()
    );

    public static CombinedTtlProvider getInstance() {
        return INSTANCE;
    }

    @VisibleForTesting
    CombinedTtlProvider(ConfigTtlProvider primary, SafetyTtlProvider safety) {
        this.primary = primary;
        this.safety = safety;
    }

    @Override
    public Optional<TimeValue> getTTL(String tenantId, Granularity gran, RollupType rollupType) {
        Optional<TimeValue> primaryValue = primary.getTTL(tenantId, gran, rollupType);
        Optional<TimeValue> safetyValue = safety.getTTL(tenantId, gran, rollupType);
        return getTimeValue(primaryValue, safetyValue);
    }

    @Override
    public Optional<TimeValue> getTTLForStrings(String tenantId) {
        Optional<TimeValue> primaryValue = primary.getTTLForStrings(tenantId);
        Optional<TimeValue> safetyValue = safety.getTTLForStrings(tenantId);
        return getTimeValue(primaryValue, safetyValue);
    }

    public long getFinalTTL(String tenantid, Granularity gran) {
        long ttl;
        if (gran == Granularity.FULL && primary.areTTLsForced()) {
            ttl = primary.getConfigTTLForIngestion().toMillis();
        } else {
            ttl = safety.getTTL(tenantid, gran, RollupType.BF_BASIC).get().toMillis();
        }
        return ttl;
    }

    private Optional<TimeValue> getTimeValue(Optional<TimeValue> primaryValue, Optional<TimeValue> safetyValue) {
        if (!primaryValue.isPresent()) {
            return safetyValue;
        }
        return primaryValue;
    }
}
