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
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;

public interface TenantTtlProvider {

    public static final int LOCATOR_TTL = 604800;   // ttl for locators in seconds, 604800s = 1 week
    public static final int DELAYED_LOCATOR_TTL = 259200;   // ttl for delayed locators in seconds, 259200s = 3 days

    public Optional<TimeValue> getTTL(String tenantId, Granularity gran, RollupType rollupType);

    public Optional<TimeValue> getTTLForStrings(String tenantId);
}
