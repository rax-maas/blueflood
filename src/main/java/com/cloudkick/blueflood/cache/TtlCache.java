package com.cloudkick.blueflood.cache;

import com.cloudkick.blueflood.internal.Account;
import com.cloudkick.blueflood.internal.ClusterException;
import com.cloudkick.blueflood.internal.InternalAPI;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.utils.TimeValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.http.client.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

// todo: need to go into safety mode if http fetches are taking too long.
public class TtlCache extends AbstractJmxCache implements TtlCacheMBean {
    private static final Logger log = LoggerFactory.getLogger(TtlCache.class);
    
    // these values get used in the absence of a ttl (internal API failure, etc.).
    static final Map<Granularity, TimeValue> SAFETY_TTLS = new HashMap<Granularity, TimeValue>() {{
        for (Granularity gran : Granularity.granularities())
            put(gran, new TimeValue(gran.getTTL().getValue() * 5, gran.getTTL().getUnit()));
    }};
    
    private final com.google.common.cache.LoadingCache<String, Map<String, TimeValue>> cache;
    private final Meter generalErrorMeter;
    private final Meter httpErrorMeter;
    
    // allowable errors per minute.
    private double safetyThreshold = 10d;
    
    private volatile long lastFetchError = 0;

    public TtlCache(String label, TimeValue expiration, int cacheConcurrency, final InternalAPI internalAPI) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format(TtlCache.class.getPackage().getName() + ":type=%s,scope=%s,name=Stats", TtlCache.class.getSimpleName(), label);
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            instantiateYammerMetrics(TtlCache.class, label, nameObj);
        } catch (Exception ex) {
            log.error("Unable to register mbean for " + getClass().getName());
        }
        generalErrorMeter = Metrics.newMeter(TtlCache.class, "Load Errors", label, "Rollups", TimeUnit.MINUTES);
        httpErrorMeter = Metrics.newMeter(TtlCache.class, "Http Errors", label, "Rollups", TimeUnit.MINUTES);
        
        
        CacheLoader<String, Map<String, TimeValue>> loader = new CacheLoader<String, Map<String, TimeValue>>() {
            
            // values from the default account are used to build a ttl map for accounts that do not exist in the 
            // internal API.  These values are put into the cache, meaning subsequent cache requests do not incur a
            // miss and hit the internal API.
            private final Account DEFAULT_ACCOUNT = new Account() {
                @Override
                public TimeValue getMetricTtl(String resolution) {
                    return SAFETY_TTLS.get(Granularity.fromString(resolution));
                }
            };
            
            @Override
            public Map<String, TimeValue> load(final String key) throws Exception {
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
                        log.error(ex.getMessage());
                        return buildTtlMap(DEFAULT_ACCOUNT);
                    } else
                        throw ex;
                }
                
            }
        };
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(expiration.getValue(), expiration.getUnit())
                .concurrencyLevel(cacheConcurrency)
                .recordStats()
                .build(loader);
    }
    
    // override this if you're not interested in caching the entire ttl map.
    protected Map<String, TimeValue> buildTtlMap(Account acct) {
        Map<String, TimeValue> map = new HashMap<String, TimeValue>();
        for (Granularity gran : Granularity.granularities())
            map.put(gran.shortName(), acct.getMetricTtl(gran.shortName()));
        return map;
    }
    
    // may return null (e.g.: if the granularity isn't in the build-map.
    public TimeValue getTtl(String acctId, Granularity gran) {
        // if error rate exceeds a threshold return SAFETY_TTL.  Otherwise, we spend a non-trivial amount of time stuck
        // in blocking calls into whatever obtains the account.
        if (isSafetyMode())
            return SAFETY_TTLS.get(gran);
        
        try {
            return cache.get(acctId).get(gran.shortName());
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
            return SAFETY_TTLS.get(gran);
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
        return generalErrorMeter.oneMinuteRate() > safetyThreshold;
    }

    public synchronized void setSafetyThreshold(double d) { safetyThreshold = d; }
    public synchronized double getSafetyThreshold() { return safetyThreshold; }
}
