package com.cloudkick.blueflood.cache;

import com.cloudkick.blueflood.internal.Account;
import com.cloudkick.blueflood.internal.AccountMapEntry;
import com.cloudkick.blueflood.internal.AccountTest;
import com.cloudkick.blueflood.internal.InternalAPI;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import junit.framework.Assert;
import org.apache.http.client.HttpResponseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TtlCacheTest {
    
    // ids in cache are: ackVCKg1rk and acAAAAAAAA
    private TtlCache twoSecondCache;
    private AtomicLong loadCount;
    private AtomicLong buildCount;
    
    @Before
    public void setupCache() {
        loadCount = new AtomicLong(0);
        buildCount = new AtomicLong(0);
        twoSecondCache = new TtlCache("Test", new TimeValue(2, TimeUnit.SECONDS), 5, new InternalAPI() {
            public Account fetchAccount(String accountId) throws IOException {
                loadCount.incrementAndGet();
                if (IOException.class.getName().equals(accountId))
                    throw new IOException("Error retrieving account from internal API");
                else if (HttpResponseException.class.getName().equals(accountId))
                    throw new HttpResponseException(404, "That account does not exist");
                else
                    return Account.fromJSON(AccountTest.JSON_ACCOUNTS.get(accountId));
            }

            @Override
            public List<AccountMapEntry> listAccountMapEntries() throws IOException {
                throw new RuntimeException("Not implemented for this test");
            }
        }) {
            @Override
            protected Map<String, TimeValue> buildTtlMap(Account acct) {
                buildCount.incrementAndGet();
                return super.buildTtlMap(acct);
            }
        };
    }
    
    @After
    public void deregister() throws Exception {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        final String name = String.format(TtlCache.class.getPackage().getName() + ":type=%s,scope=%s,name=Stats", TtlCache.class.getSimpleName(), "Test");
        final ObjectName nameObj = new ObjectName(name);
        mbs.unregisterMBean(nameObj);
        
        // involves a little unsafe knowledge of TtlCache internals.
        Metrics.defaultRegistry().removeMetric(TtlCache.class, "Load Errors", "Test");
        Metrics.defaultRegistry().removeMetric(TtlCache.class, "Http Errors", "Test");
    }
    
    private void warmCache() {
        for (int i = 0; i < 100; i++) {
            for (Granularity gran : Granularity.granularities()) {
                twoSecondCache.getTtl("ackVCKg1rk", gran);
                twoSecondCache.getTtl("acAAAAAAAA", gran);
            }
        }
    }
    
    @Test
    public void testCacheDoesNotReloadOrRebuild() {
        warmCache();
        Assert.assertEquals(2, loadCount.get());
        Assert.assertEquals(2, buildCount.get());
        warmCache();
        Assert.assertEquals(2, loadCount.get());
        Assert.assertEquals(2, buildCount.get());
    }
    
    @Test
    public void testCacheReloadAfterExpiry() throws Exception {
        warmCache();
        Assert.assertEquals(2, loadCount.get());
        Assert.assertEquals(2, buildCount.get());
        Thread.sleep(3000);
        warmCache();
        Assert.assertEquals(4, loadCount.get());
        Assert.assertEquals(4, buildCount.get());
        warmCache();
    }
    
    @Test
    public void testDefaultsAreNotNormallyUsed() {
        warmCache();
        for (String acctId : new String[] {"ackVCKg1rk", "acAAAAAAAA"})
            for (Granularity gran : Granularity.granularities())
                Assert.assertFalse(TtlCache.SAFETY_TTLS.get(gran).toSeconds() == twoSecondCache.getTtl(acctId, gran).toSeconds());
    }
    
    @Test
    public void testDefaultsUsedOnAPIError() {
        warmCache();
        for (Map.Entry<Granularity, TimeValue> entry : TtlCache.SAFETY_TTLS.entrySet())
            Assert.assertEquals(entry.getValue(), twoSecondCache.getTtl(IOException.class.getName(), entry.getKey()));
    }
    
    @Test
    public void testDefaultUsedOnHttpError() {
        warmCache();
        for (Map.Entry<Granularity, TimeValue> entry : TtlCache.SAFETY_TTLS.entrySet())
            Assert.assertEquals(entry.getValue(), twoSecondCache.getTtl(HttpResponseException.class.getName(), entry.getKey()));
    }
    
    @Test
    public void testIOErrorTriggersSafetyMode() {
        warmCache();
        for (int i = 0; i < (int)twoSecondCache.getSafetyThreshold() + 10; i++)
            twoSecondCache.getTtl(IOException.class.getName(), Granularity.FULL);
        forceMeterTick("generalErrorMeter");
        Assert.assertTrue(twoSecondCache.isSafetyMode());
    }
    
    @Test
    public void testHttpErrorDoesNotTriggerSafetyMode() {
        warmCache();
        for (int i = 0; i < (int)twoSecondCache.getSafetyThreshold() + 1; i++)
            twoSecondCache.getTtl(HttpResponseException.class.getName(), Granularity.FULL);
        forceMeterTick("generalErrorMeter");
        Assert.assertFalse(twoSecondCache.isSafetyMode());
    }
    
    // normally this is handled by a thread in the yammer library.  The point here is to force it.
    private void forceMeterTick(String fieldName) {
        try {
            Field generalErrorMeterField = TtlCache.class.getDeclaredField(fieldName);
            generalErrorMeterField.setAccessible(true);
            Object generalErrorMeter = generalErrorMeterField.get(twoSecondCache);
            Method tick = generalErrorMeter.getClass().getDeclaredMethod("tick");
            tick.setAccessible(true);
            tick.invoke(generalErrorMeter);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
