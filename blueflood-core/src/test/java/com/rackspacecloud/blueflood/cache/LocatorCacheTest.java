package com.rackspacecloud.blueflood.cache;

import com.rackspacecloud.blueflood.types.Locator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class LocatorCacheTest {

    private static final Locator LOCATOR = Locator.createLocatorFromDbKey("a.b.c.d");
    private LocatorCache locatorCache;

    @Before
    public void setup() {
        locatorCache = LocatorCache.getInstance(2L, TimeUnit.SECONDS, 3L, TimeUnit.SECONDS);

    }

    @After
    public void tearDown() {
        locatorCache.resetCache();
    }

    @Test
    public void testSetLocatorCurrentInBatchLayer() throws InterruptedException {

        locatorCache.setLocatorCurrentInBatchLayer(LOCATOR);

        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

        Thread.sleep(2000L); //sleeping for 2 seconds to let the locator expire from cache

        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));
    }

    @Test
    public void testSetLocatorCurrentInBatchLayerCheckForExpirationWithoutReads() throws InterruptedException {

        locatorCache.setLocatorCurrentInBatchLayer(LOCATOR);

        Thread.sleep(3001L);
        // it has been more than 3 seconds since it was written.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

    }

    @Test
    public void testSetLocatorCurrentInBatchLayerCheckForExpiration() throws InterruptedException {

        locatorCache.setLocatorCurrentInBatchLayer(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

        Thread.sleep(1001L);
        // it has been more than 3 seconds since it was written. So it has to expire even if it was accessed 1sec before.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

    }

    @Test
    public void testIsLocatorCurrentInBatchLayer() throws InterruptedException {
        assertTrue("locator which was never set is present in cache", !locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

        locatorCache.setLocatorCurrentInBatchLayer(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInBatchLayer(LOCATOR));

    }

    @Test
    public void testSetLocatorCurrentInDiscoveryLayer() throws InterruptedException {

        locatorCache.setLocatorCurrentInDiscoveryLayer(LOCATOR);

        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

        Thread.sleep(2000L); //sleeping for 2 seconds to let the locator expire from cache

        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));
    }

    @Test
    public void testSetLocatorCurrentInDiscoveryLayerCheckForExpirationWithoutReads() throws InterruptedException {

        locatorCache.setLocatorCurrentInDiscoveryLayer(LOCATOR);

        Thread.sleep(3001L);
        // it has been more than 3 seconds since it was written.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

    }

    @Test
    public void testSetLocatorCurrentInDiscoveryLayerCheckForExpiration() throws InterruptedException {

        locatorCache.setLocatorCurrentInDiscoveryLayer(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

        Thread.sleep(1001L);
        // it has been more than 3 seconds since it was written. So it has to expire even if it was accessed 1sec before.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

    }

    @Test
    public void testIsLocatorCurrentInDiscoveryLayer() throws InterruptedException {
        assertTrue("locator which was never set is present in cache", !locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

        locatorCache.setLocatorCurrentInDiscoveryLayer(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrentInDiscoveryLayer(LOCATOR));

    }


    @Test
    public void testSetDelayedLocatorCurrent() throws InterruptedException {
        locatorCache.setDelayedLocatorForASlotCurrent(1, LOCATOR);
        locatorCache.setDelayedLocatorForASlotCurrent(2, LOCATOR);

        assertTrue("locator not stored in cache", locatorCache.isDelayedLocatorForASlotCurrent(1, LOCATOR));

        Thread.sleep(2000L); //sleeping for 2 seconds to let the locator expire from cache

        assertTrue("locator not expired from cache", !locatorCache.isDelayedLocatorForASlotCurrent(1, LOCATOR));
        assertTrue("locator not expired from cache", !locatorCache.isDelayedLocatorForASlotCurrent(2, LOCATOR));
    }

    @Test
    public void testIsDelayedLocatorCurrent() throws InterruptedException {
        assertTrue("locator not stored in cache", !locatorCache.isDelayedLocatorForASlotCurrent(1, LOCATOR));

        locatorCache.setDelayedLocatorForASlotCurrent(1, LOCATOR);

        assertTrue("locator not stored in cache", locatorCache.isDelayedLocatorForASlotCurrent(1, LOCATOR));
        assertTrue("locator present in cache for a slot which was never set", !locatorCache.isDelayedLocatorForASlotCurrent(10, LOCATOR));
    }
}
