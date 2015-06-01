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
import com.rackspacecloud.blueflood.types.RollupType;
import com.rackspacecloud.blueflood.utils.TimeValue;

public interface TenantTtlProvider {
    public TimeValue getTTL(String tenantId, Granularity gran, RollupType rollupType) throws Exception;

    public void setTTL(String tenantId, Granularity gran, RollupType rollupType, TimeValue ttlValue) throws Exception;

    public TimeValue getTTLForStrings(String tenantId) throws Exception;

    public TimeValue getConfigTTLForIngestion() throws Exception;
}
