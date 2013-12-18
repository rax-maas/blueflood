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
import com.codahale.metrics.MetricRegistry;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.internal.Account;
import com.rackspacecloud.blueflood.internal.AccountMapEntry;
import com.rackspacecloud.blueflood.internal.AccountTest;
import com.rackspacecloud.blueflood.internal.InternalAPI;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.http.client.HttpResponseException;
import org.junit.After;
import org.junit.Assert;
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
    private TestClock testClock;
    
    @Before
    public void setupCache() {
        loadCount = new AtomicLong(0);
        buildCount = new AtomicLong(0);
        testClock = new TestClock();
        twoSecondCache = new TtlCache("Test", new TimeValue(2, TimeUnit.SECONDS), 5, new InternalAPI() {
            public Account fetchAccount(String tenantId) throws IOException {
                loadCount.incrementAndGet();
                if (IOException.class.getName().equals(tenantId))
                    throw new IOException("Error retrieving account from internal API");
                else if (HttpResponseException.class.getName().equals(tenantId))
                    throw new HttpResponseException(404, "That account does not exist");
                else
                    return Account.fromJSON(AccountTest.JSON_ACCOUNTS.get(tenantId));
            }

            @Override
            public List<AccountMapEntry> listAccountMapEntries() throws IOException {
                throw new RuntimeException("Not implemented for this test");
            }
        }, testClock) {
            @Override
            protected Map<ColumnFamily<Locator, Long>, TimeValue> buildTtlMap(Account acct) {
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
        Metrics.getRegistry().remove(MetricRegistry.name(TtlCache.class, "Test", "Load Errors"));
        Metrics.getRegistry().remove(MetricRegistry.name(TtlCache.class, "Test", "Http Errors"));
    }
    
    private void warmCache() {
        for (int i = 0; i < 100; i++) {
            for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                twoSecondCache.getTtl("ackVCKg1rk", cf);
                twoSecondCache.getTtl("acAAAAAAAA", cf);
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
            for (Granularity gran : Granularity.granularities()) {
                ColumnFamily<Locator, Long> CF = CassandraModel.getColumnFamily(BasicRollup.class, gran);
                Assert.assertFalse("Failed for CF: " + CF.getName(),
                        TtlCache.SAFETY_TTLS.get(CF).toSeconds() == twoSecondCache.getTtl(acctId, CF).toSeconds());
            }
    }
    
    @Test
    public void testDefaultsUsedOnAPIError() {
        warmCache();
        for (Map.Entry<ColumnFamily<Locator, Long>, TimeValue> entry : TtlCache.SAFETY_TTLS.entrySet()) {
            Assert.assertEquals(entry.getValue(), twoSecondCache.getTtl(IOException.class.getName(), entry.getKey()));
        }
    }
    
    @Test
    public void testDefaultUsedOnHttpError() {
        warmCache();
        for (Map.Entry<ColumnFamily<Locator, Long>, TimeValue> entry : TtlCache.SAFETY_TTLS.entrySet()) {
            Assert.assertEquals(entry.getValue(), twoSecondCache.getTtl(HttpResponseException.class.getName(), entry.getKey()));
        }
    }
    
    @Test
    public void testIOErrorTriggersSafetyMode() {
        warmCache();
        for (int i = 0; i < (int)twoSecondCache.getSafetyThreshold() + 10; i++) {
            twoSecondCache.getTtl(IOException.class.getName(), CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL));
        }
        forceMeterTick("generalErrorMeter");
        Assert.assertTrue(twoSecondCache.isSafetyMode());
    }
    
    @Test
    public void testHttpErrorDoesNotTriggerSafetyMode() {
        warmCache();
        for (int i = 0; i < (int)twoSecondCache.getSafetyThreshold() + 1; i++) {
            twoSecondCache.getTtl(HttpResponseException.class.getName(),
                    CassandraModel.getColumnFamily(BasicRollup.class, Granularity.FULL));
        }
        forceMeterTick("generalErrorMeter");
        Assert.assertFalse(twoSecondCache.isSafetyMode());
    }
    
    // normally this is handled by a thread in the yammer library.  The point here is to force it.
    private void forceMeterTick(String fieldName) {
        try {
            Field generalErrorMeterField = TtlCache.class.getDeclaredField(fieldName);
            generalErrorMeterField.setAccessible(true);
            Object generalErrorMeter = generalErrorMeterField.get(twoSecondCache);
            testClock.setTime(testClock.getTick() + TimeUnit.SECONDS.toNanos(5l));

            Method tick = generalErrorMeter.getClass().getDeclaredMethod("tickIfNecessary");
            tick.setAccessible(true);
            tick.invoke(generalErrorMeter);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public class TestClock extends Clock {
        private Long override = null;
        public TestClock() {

        }

        @Override
        public long getTick() {
            if (override == null) {
                return System.nanoTime();
            } else {
                return override;
            }
        }

        public void setTime(long nanos) {
            this.override = nanos;
        }
    }
}
