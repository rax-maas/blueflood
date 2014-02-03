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

package com.rackspacecloud.blueflood.cache;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.internal.Account;
import com.rackspacecloud.blueflood.internal.ClusterException;
import com.rackspacecloud.blueflood.internal.InternalAPI;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.http.client.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

// todo: need to go into safety mode if http fetches are taking too long.
public class TtlCache extends AbstractJmxCache implements TtlCacheMBean {
    private static final Logger log = LoggerFactory.getLogger(TtlCache.class);
    
    // these values get used in the absence of a ttl (internal API failure, etc.).
    static final Map<ColumnFamily<Locator, Long>, TimeValue> SAFETY_TTLS =
            new HashMap<ColumnFamily<Locator, Long>, TimeValue>() {{
                for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                    put(cf, new TimeValue(cf.getDefaultTTL().getValue() * 5, cf.getDefaultTTL().getUnit()));
                }
            }};
    
    private final com.google.common.cache.LoadingCache<String, Map<ColumnFamily<Locator, Long>, TimeValue>> cache;
    private final Meter generalErrorMeter;
    private final Meter httpErrorMeter;
    
    // allowable errors per minute.
    private double safetyThreshold = 10d;
    
    private volatile long lastFetchError = 0;
    private final int concurrency;

    public TtlCache(String label, TimeValue expiration, int cacheConcurrency, final InternalAPI internalAPI) {
        this(label, expiration, cacheConcurrency, internalAPI, Clock.defaultClock());
    }

    protected TtlCache(String label, TimeValue expiration, int cacheConcurrency, final InternalAPI internalAPI, Clock clock) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format(TtlCache.class.getPackage().getName() + ":type=%s,scope=%s,name=Stats", TtlCache.class.getSimpleName(), label);
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            instantiateYammerMetrics(TtlCache.class, label, nameObj);
        } catch (Exception ex) {
            log.error("Unable to register mbean for " + getClass().getName());
        }

        httpErrorMeter = Metrics.meter(TtlCache.class, label, "Http Errors");
        generalErrorMeter = Metrics.getRegistry().register(MetricRegistry.name(TtlCache.class, label, "Load Errors"),
                new Meter(clock)); // because TtlCacheTest
        
        
        CacheLoader<String, Map<ColumnFamily<Locator, Long>, TimeValue>> loader =
                new CacheLoader<String, Map<ColumnFamily<Locator, Long>, TimeValue>>() {
            
                    // values from the default account are used to build a ttl map for tenants that do not exist in the
                    // internal API.  These values are put into the cache, meaning subsequent cache requests do not
                    // incur a miss and hit the internal API.
                    private final Account DEFAULT_ACCOUNT = new Account() {
                        @Override
                        public TimeValue getMetricTtl(String resolution) {
                            return SAFETY_TTLS.get(CassandraModel.getColumnFamily(BasicRollup.class,
                                   Granularity.fromString(resolution)));
                        }
                    };

                    @Override
                    public Map<ColumnFamily<Locator, Long>, TimeValue> load(final String key) throws Exception {
                        // load account, build ttl map.
                        try {
                            Account acct = internalAPI.fetchAccount(key);
                            return buildTtlMap(acct);
                        } catch (HttpResponseException ex) {
                            // cache the default value on a 404. this means that we will not be hammering the API for values
                            // that are constantly not there.  The other option was to let the Http error bubble out, use a
                            // and value from SAFETY_TTLS.  But the same thing (an HTTP round trip) would happen the very next
                            // time a TTL is requested.
                            if (ex.getStatusCode() == 404) {
                                httpErrorMeter.mark();
                                log.warn(ex.getMessage());
                                return buildTtlMap(DEFAULT_ACCOUNT);
                            } else
                                throw ex;
                        }

                    }
                };
        this.concurrency = cacheConcurrency;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiration.getValue(), expiration.getUnit())
                .concurrencyLevel(concurrency)
                .recordStats()
                .build(loader);
    }

    public int getConcurrency() {
        return concurrency;
    }

    // XXX: This is horribly broken. Do not use this.
    // override this if you're not interested in caching the entire ttl map.
    protected Map<ColumnFamily<Locator, Long>, TimeValue> buildTtlMap(Account acct) {
        Map<ColumnFamily<Locator, Long>, TimeValue> map = new HashMap<ColumnFamily<Locator, Long>, TimeValue>();
        for (Granularity gran : Granularity.granularities())
            map.put(CassandraModel.getColumnFamily(BasicRollup.class, gran), acct.getMetricTtl(gran.shortName()));
        // todo: this is a temporary hack.  The Account API maps TTL to granularity, but everywhere else TTL is linked
        // to column families.  Either way, the account api used at rackspace doesn't contain TTLs for the string CF.
        map.put(CassandraModel.CF_METRICS_STRING, SAFETY_TTLS.get(CassandraModel.CF_METRICS_STRING));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_FULL, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_FULL));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_5M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_5M));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_20M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_20M));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_60M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_60M));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_240M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_240M));
        map.put(CassandraModel.CF_METRICS_PREAGGREGATED_1440M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_PREAGGREGATED_1440M));
        map.put(CassandraModel.CF_METRICS_HIST_5M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_HIST_5M));
        map.put(CassandraModel.CF_METRICS_HIST_20M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_HIST_20M));
        map.put(CassandraModel.CF_METRICS_HIST_60M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_HIST_60M));
        map.put(CassandraModel.CF_METRICS_HIST_240M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_HIST_240M));
        map.put(CassandraModel.CF_METRICS_HIST_1440M, SAFETY_TTLS.get(CassandraModel.CF_METRICS_HIST_1440M));

        return map;
    }
    
    // may return null (e.g.: if the granularity isn't in the build-map.
    public TimeValue getTtl(String acctId, ColumnFamily<Locator, Long> CF) {
        // if error rate exceeds a threshold return SAFETY_TTL.  Otherwise, we spend a non-trivial amount of time stuck
        // in blocking calls into whatever obtains the account.
        if (isSafetyMode())
            return SAFETY_TTLS.get(CF);
        
        try {
            return cache.get(acctId).get(CF);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof HttpResponseException) {
                httpErrorMeter.mark();
                log.debug(ex.getCause().getMessage());
            } else {
                // log this every 10s, then use a sane default value for the ttl.
                generalErrorMeter.mark();
                long now = System.currentTimeMillis();
                if (now - lastFetchError > 2000) {
                    log.error("Problem fetching accounts from internal API");
                    if (ex.getCause() instanceof ClusterException) {
                        for (Throwable subCause : ((ClusterException)ex.getCause()).getExceptions())
                            log.error("Subcause: " + subCause.getMessage());
                    } else
                        log.error(ex.getCause().getMessage());
                    lastFetchError = now; // I don't care about races here.
                }
            }
            return SAFETY_TTLS.get(CF);
        }
    }
    
    //
    // Jmx methods
    //


    @Override
    public CacheStats getStats() {
        return cache.stats();
    }

    public synchronized boolean isSafetyMode() {
        // oneMinuteRate is the per-second rate, so multiply by 60.
        return generalErrorMeter.getOneMinuteRate() * 60d > safetyThreshold;
    }

    public synchronized void setSafetyThreshold(double d) { safetyThreshold = d; }
    public synchronized double getSafetyThreshold() { return safetyThreshold; }
}
