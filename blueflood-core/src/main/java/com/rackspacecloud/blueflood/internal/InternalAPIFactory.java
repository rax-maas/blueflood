/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.internal;

import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.conn.PoolingClientConnectionManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InternalAPIFactory {
    public static final String BASE_PATH = "/v1.0";
    private static int PAGINATION_LIMIT = 1000;
    private static int PAGINATION_RETRY_COUNT = 3;

    static final Map<Granularity, TimeValue> SAFETY_TTLS = new HashMap<Granularity, TimeValue>() {{
        for (Granularity gran : Granularity.granularities()) {
            TimeValue defaultTTL = ((CassandraModel.MetricColumnFamily) CassandraModel.getColumnFamily(BasicRollup.class, gran)).getDefaultTTL();
            put(gran, new TimeValue(defaultTTL.getValue() * 5, defaultTTL.getUnit()));
        }
    }};

    private static final Account DEFAULT_ACCOUNT = new Account() {
        @Override
        public TimeValue getMetricTtl(String resolution) {
            return SAFETY_TTLS.get(Granularity.fromString(resolution));
        }
    };
    
    static ClientConnectionManager buildConnectionManager(int concurrency) {
        final PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(concurrency);
        connectionManager.setMaxTotal(concurrency);
        return connectionManager;
    }
    
    public static InternalAPI create(final int concurrency, final String clusterString) {
        return new InternalAPI() {
            // NOTE: some tests (InternalAPITest) make assumptions about these members being here.
            final ClientConnectionManager connectionManager = buildConnectionManager(concurrency);  
            final JsonResource jsonResource = new HttpJsonResource(connectionManager, clusterString, BASE_PATH);
            
            public Account fetchAccount(String tenantId) throws IOException {
                return Account.fromJSON(jsonResource.getResource("/accounts/" + tenantId));
            }

            public List<AccountMapEntry> listAccountMapEntries() throws IOException {
                List<AccountMapEntry> entries = new LinkedList<AccountMapEntry>();
                String nextMarker = null;

                do {
                    String path = "/accounts?limit=" + PAGINATION_LIMIT;

                    if (nextMarker != null) {
                        path += "&marker=" + nextMarker;
                    }

                    PaginatedAccountMap result = PaginatedAccountMap.fromJSON(jsonResource.getResource(path));
                    entries.addAll(result.getEntries());
                    nextMarker = result.getNextMarker();

                } while (nextMarker != null);

                return entries;
            }
        };
    }

    public static InternalAPI createDefaultTTLProvider() {
        return new InternalAPI() {
            public Account fetchAccount(String tenantId) throws IOException {
                return DEFAULT_ACCOUNT;
            }

            public List<AccountMapEntry> listAccountMapEntries() throws IOException {
                throw new RuntimeException("Not implemented");
            }
        };
    }
}
