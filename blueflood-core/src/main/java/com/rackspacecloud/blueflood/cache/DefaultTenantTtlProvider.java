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
import com.rackspacecloud.blueflood.types.TtlMapper;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DefaultTenantTtlProvider implements TenantTtlProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultTenantTtlProvider.class);

    private final ConfigTtlProvider fallback =  ConfigTtlProvider.getInstance();
    private final LoadingCache<String, TtlMapper> tenanttTtlMap;

    public DefaultTenantTtlProvider(TimeValue expiration, int cacheConcurrency) {
        CacheLoader<String, TtlMapper> loader = new CacheLoader<String, TtlMapper>() {

            @Override
            public TtlMapper load(final String tenantId) throws Exception {
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
}
