package com.rackspacecloud.blueflood.cache;

import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import com.rackspacecloud.blueflood.types.Locator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LocatorCacheTest {

    @Rule
    public RepeatingRule repeatedly = new RepeatingRule();

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
    public void expiresStuff() throws Exception {
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
        Thread.sleep(101);
        locators.forEach(locator -> {
            assertThat(cache.isLocatorCurrentInBatchLayer(locator), is(false));
            assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(false));
        });
    }

    @Test
    public void expiresStuffThatsNeverTouched() throws Exception {
        LocatorCache cache = LocatorCache.getInstance(100, MILLISECONDS,100, MILLISECONDS);
        Locator locator = Locator.createLocatorFromDbKey("never.touched");
        cache.setLocatorCurrentInBatchLayer(locator);
        cache.setLocatorCurrentInDiscoveryLayer(locator);
        cache.setLocatorCurrentInTokenDiscoveryLayer(locator);
        cache.setDelayedLocatorForASlotCurrent(1, locator);
        Thread.sleep(101);
        assertThat(cache.isLocatorCurrentInBatchLayer(locator), is(false));
        assertThat(cache.isLocatorCurrentInDiscoveryLayer(locator), is(false));
        assertThat(cache.isLocatorCurrentInTokenDiscoveryLayer(locator), is(false));
        assertThat(cache.isDelayedLocatorForASlotCurrent(1, locator), is(false));
    }

    private static class Record {
        public final Locator locator;
        public final LocatorCache.Layer layer;

        public Record(Locator locator, LocatorCache.Layer layer) {
            this.locator = locator;
            this.layer = layer;
        }
    }

    @Test
    // Repeat in order to exercise multiple values for the various random settings in the test, providing greater
    // overall coverage.
    @Repeating(repetition = 25)
    public void handlesRandomConcurrencyLoad() {
        Random r = new Random();
        ConcurrentLinkedQueue<LocatorCacheTest.Record> myRecords = new ConcurrentLinkedQueue<>();
        int concurrency = 2 + r.nextInt(20);
        int executions = 1 + r.nextInt(1000);
        System.out.println("Test concurrency with " + concurrency + " threads and " + executions + " executions");
        doConcurrently(concurrency, executions, () -> {
            // For each execution, put a random locator in a random layer
            Locator locator = Locator.createLocatorFromDbKey("foo." + r.nextInt(1000));
            LocatorCache.Layer layer = LocatorCache.Layer.values()[r.nextInt(3)];
            if (!cache.isLocatorCurrentInLayer(locator, layer)) {
                cache.setLocatorCurrentInLayer(locator, layer);
            }
            // Remember what we did
            myRecords.offer(new LocatorCacheTest.Record(locator, layer));
        });
        // Sanity check our data
        assertThat(myRecords.size(), equalTo(executions));
        // Now verify that everything we set is remembered
        for (LocatorCacheTest.Record record : myRecords) {
            assertThat(record.locator, isCurrentIn(cache, record.layer));
        }
    }

    private static Matcher<Locator> isCurrentIn(LocatorCache cache, LocatorCache.Layer layer) {
        return new IsCurrentInMatcher(cache, layer);
    }

    private static class IsCurrentInMatcher extends BaseMatcher<Locator> {
        private final LocatorCache cache;
        private final LocatorCache.Layer layer;

        public IsCurrentInMatcher(LocatorCache cache, LocatorCache.Layer layer) {
            this.cache = cache;
            this.layer = layer;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof Locator)) {
                return false;
            }
            Locator locator = (Locator) item;
            return cache.isLocatorCurrentInLayer(locator, layer);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a locator value that's current in the '" + layer + "' layer");
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            description
                    .appendValue(item)
                    .appendText(" is missing; layer contains: ")
                    .appendValue(cache.getAllCurrentInLayer(layer));
        }
    }

    @Test
    // DO NOT synchronize this cache. It bottlenecks all the database writing threads and kills write performance!
    // This is difficult to show in a unit test, but it's clearly observable in production.
    public void mustNotBeSynchronized() throws IOException {
        Path path = Paths.get("src/main/java/com/rackspacecloud/blueflood/cache/LocatorCache.java");
        for (String line : Files.readAllLines(path)) {
            if (line.contains("synchronized")) {
                Assert.fail("Found 'synchronized' in line:\n" + line);
            }
        }
    }

    /**
     * Test helper that executes a given runnable multiple times in multiple threads. This was written, rather than
     * using tempus fugit's @Concurrent annotation, because I want to run lots of operations on the cache in parallel,
     * then verify the results, and then I want to repeat that test several times.
     *
     * @param concurrency how many threads will run at the same time
     * @param executions  how many times to execute the runnable
     * @param runnable    the runnable to execute
     */
    private void doConcurrently(int concurrency, int executions, Runnable runnable) {
        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        CountDownLatch starter = new CountDownLatch(1);
        for (int i = 0; i < executions; i++) {
            executorService.submit(() -> {
                starter.await();
                runnable.run();
                return true;
            });
        }
        try {
            // let all threads get ready
            Thread.sleep(100);
            // signal all threads to start
            starter.countDown();
            // wait for all threads to finish
            executorService.shutdown();
            assertThat(executorService.awaitTermination(10, SECONDS), is(true));
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected interrupt", e);
        }
    }
}
