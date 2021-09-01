package com.rackspacecloud.blueflood.cache;

import com.google.code.tempusfugit.temporal.Condition;
import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static com.google.code.tempusfugit.temporal.Duration.millis;
import static com.google.code.tempusfugit.temporal.Timeout.timeout;
import static com.google.code.tempusfugit.temporal.WaitFor.waitOrTimeout;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class LocatorCacheTest {

    private final LocatorCache cache = LocatorCache.getInstance(
            60, SECONDS, 60, SECONDS);

    @Before
    public void setUp() {
        cache.resetCache();
    }

    /**
     * Test helper that returns the current-ness of a locator in the cache in all layers. For simplicity, they're
     * returned in alphabetical order of the layer names: batch, discovery, token discovery. A test should order itself
     * in this way to keep things understandable.
     *
     * @param locator Locator value
     * @param cache   LocatorCache to check
     * @return a 3-length list where each item is a boolean describing the current-ness of the locator in the layer
     */
    public List<Boolean> getCurrents(Locator locator, LocatorCache cache) {
        return Arrays.asList(
                cache.isLocatorCurrentInBatchLayer(locator),
                cache.isLocatorCurrentInDiscoveryLayer(locator),
                cache.isLocatorCurrentInTokenDiscoveryLayer(locator)
        );
    }

    /**
     * Test helper to shorten test code. Just creates a list of the inputs that can be compared to the result of
     * {@link #getCurrents(Locator, LocatorCache)}
     */
    private List<Boolean> list(Boolean... b) {
        return Arrays.asList(b);
    }

    @Test
    public void tracksCurrencyOfOneLocatorAcrossLayers() {
        Locator locator = Locator.createLocatorFromDbKey("a.b.c.d");
        assertThat(getCurrents(locator, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInBatchLayer(locator);
        assertThat(getCurrents(locator, cache), equalTo(list(true, false, false)));
        cache.setLocatorCurrentInDiscoveryLayer(locator);
        assertThat(getCurrents(locator, cache), equalTo(list(true, true, false)));
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator);
        assertThat(getCurrents(locator, cache), equalTo(list(true, true, true)));
    }

    @Test
    public void tracksCurrencyOfMultipleLocatorsInBatchLayer() {
        Locator locator1 = Locator.createLocatorFromDbKey("1.1.1");
        Locator locator2 = Locator.createLocatorFromDbKey("2.2.2");
        Locator locator3 = Locator.createLocatorFromDbKey("3.3.3");
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInBatchLayer(locator1);
        assertThat(getCurrents(locator1, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInBatchLayer(locator2);
        assertThat(getCurrents(locator1, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInBatchLayer(locator3);
        assertThat(getCurrents(locator1, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(true, false, false)));
    }

    @Test
    public void tracksCurrencyOfMultipleLocatorsInDiscoveryLayer() {
        Locator locator1 = Locator.createLocatorFromDbKey("1.1.1");
        Locator locator2 = Locator.createLocatorFromDbKey("2.2.2");
        Locator locator3 = Locator.createLocatorFromDbKey("3.3.3");
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInDiscoveryLayer(locator1);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInDiscoveryLayer(locator2);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInDiscoveryLayer(locator3);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, true, false)));
    }

    @Test
    // Repetitive? Yes. The rule of three suggests refactoring here. We could possibly introduce a Layer enum to the
    // cache interface. Would that make it less easy to use than having a separate method for each layer?
    public void tracksCurrencyOfMultipleLocatorsInTokenDiscoveryLayer() {
        Locator locator1 = Locator.createLocatorFromDbKey("1.1.1");
        Locator locator2 = Locator.createLocatorFromDbKey("2.2.2");
        Locator locator3 = Locator.createLocatorFromDbKey("3.3.3");
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator1);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, true)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator2);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, true)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, true)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator3);
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, true)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, true)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, true)));
    }

    @Test
    public void canInvalidateTheLayerCacheValues() {
        // When I set a different locator to current in each layer
        Locator locator1 = Locator.createLocatorFromDbKey("1.1.1");
        Locator locator2 = Locator.createLocatorFromDbKey("2.2.2");
        Locator locator3 = Locator.createLocatorFromDbKey("3.3.3");
        cache.setLocatorCurrentInBatchLayer(locator1);
        cache.setLocatorCurrentInDiscoveryLayer(locator2);
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator3);
        // Then the cache reflects that
        assertThat(cache.getCurrentLocatorCount(), equalTo(3L));
        assertThat(getCurrents(locator1, cache), equalTo(list(true, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, true, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, true)));
        // When I reset the cache
        cache.resetInsertedLocatorsCache();
        // Then it all goes away
        assertThat(cache.getCurrentLocatorCount(), equalTo(0L));
        assertThat(getCurrents(locator1, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator2, cache), equalTo(list(false, false, false)));
        assertThat(getCurrents(locator3, cache), equalTo(list(false, false, false)));
    }

    @Test
    public void tracksDelayedLocatorsBySlot() {
        Locator locator = Locator.createLocatorFromDbKey("i.m.delayed");
        assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(false));
        assertThat(cache.isDelayedLocatorForASlotCurrent(11, locator), is(false));
        assertThat(cache.isDelayedLocatorForASlotCurrent(999, locator), is(false));
        cache.setDelayedLocatorForASlotCurrent(1, locator);
        assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(true));
        assertThat(cache.isDelayedLocatorForASlotCurrent(11, locator), is(false));
        assertThat(cache.isDelayedLocatorForASlotCurrent(999, locator), is(false));
        cache.setDelayedLocatorForASlotCurrent(11, locator);
        assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(true));
        assertThat(cache.isDelayedLocatorForASlotCurrent(11, locator), is(true));
        assertThat(cache.isDelayedLocatorForASlotCurrent(999, locator), is(false));
        cache.setDelayedLocatorForASlotCurrent(999, locator);
        assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(true));
        assertThat(cache.isDelayedLocatorForASlotCurrent(11, locator), is(true));
        assertThat(cache.isDelayedLocatorForASlotCurrent(999, locator), is(true));
    }

    @Test
    public void canInvalidateDelayedLocatorSlots() {
        Locator locator = Locator.createLocatorFromDbKey("i.m.2");
        cache.setDelayedLocatorForASlotCurrent(1, locator);
        cache.setDelayedLocatorForASlotCurrent(123, locator);
        cache.setDelayedLocatorForASlotCurrent(90210, locator);
        assertThat(cache.getCurrentDelayedLocatorCount(), equalTo(3L));
        cache.resetCache();
        assertThat(cache.getCurrentDelayedLocatorCount(), equalTo(0L));
    }

    @Test
    public void expiresStuff() throws InterruptedException, TimeoutException {
        // Given several random locators
        Random r = new Random();
        List<Locator> locators = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            locators.add(Locator.createLocatorFromDbKey("random." + r.nextInt(100)));
        }
        // And a cache with a fairly short timeout
        LocatorCache cache = LocatorCache.getInstance(
                100, MILLISECONDS, 100, MILLISECONDS);
        // When I insert all the locators into the cache
        locators.forEach(locator -> {
            cache.setLocatorCurrentInBatchLayer(locator);
            cache.setDelayedLocatorForASlotCurrent(1, locator);
        });
        // Then the cache reflects all of the entries
        locators.forEach(locator -> {
            assertThat(cache.isLocatorCurrentInBatchLayer(locator), is(true));
            assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(true));
        });
        // And after the timeout, all of them are no longer cached
        waitOrTimeout(
                () -> locators.stream().noneMatch(cache::isLocatorCurrentInBatchLayer),
                timeout(millis(200)));
        locators.forEach(locator ->
                assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(false)));
    }

    @Test
    // DO NOT synchronize this cache. It bottlenecks all the database writing threads and kills write performance!
    // This is difficult to show in a unit test, but it's clearly observable in production.
    public void mustNotBeSynchronized() throws IOException {
        Path path = Paths.get("src/main/java/com/rackspacecloud/blueflood/cache/LocatorCache.java");
        for (String line : Files.readAllLines(path)) {
            if (line.contains("synchronized")) {
                fail("Found 'synchronized' in line:\n" + line);
            }
        }
    }
}
