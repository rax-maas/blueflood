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
    public void testSetLocatorCurrent() throws InterruptedException {

        locatorCache.setLocatorCurrent(LOCATOR);

        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrent(LOCATOR));

        Thread.sleep(2000L); //sleeping for 2 seconds to let the locator expire from cache

        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrent(LOCATOR));
    }

    @Test
    public void testSetLocatorCurrentCheckForExpirationWithoutReads() throws InterruptedException {

        locatorCache.setLocatorCurrent(LOCATOR);

        Thread.sleep(3001L);
        // it has been more than 3 seconds since it was written.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrent(LOCATOR));

    }

    @Test
    public void testSetLocatorCurrentCheckForExpiration() throws InterruptedException {

        locatorCache.setLocatorCurrent(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrent(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrent(LOCATOR));

        Thread.sleep(1000L);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrent(LOCATOR));

        Thread.sleep(1001L);
        // it has been more than 3 seconds since it was written. So it has to expire even if it was accessed 1sec before.
        assertTrue("locator not expired from cache", !locatorCache.isLocatorCurrent(LOCATOR));

    }

    @Test
    public void testIsLocatorCurrent() throws InterruptedException {
        assertTrue("locator which was never set is present in cache", !locatorCache.isLocatorCurrent(LOCATOR));

        locatorCache.setLocatorCurrent(LOCATOR);
        assertTrue("locator not stored in cache", locatorCache.isLocatorCurrent(LOCATOR));

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
