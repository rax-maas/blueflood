package com.rackspacecloud.blueflood.cache;

import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class TokenCacheTest {

    @Rule
    public RepeatingRule repeatedly = new RepeatingRule();

    private final TokenCache cache = TokenCache.getInstance(60, SECONDS);

    @Before
    public void setUp() {
        cache.resetCache();
    }

    @Test
    public void basicStuffWorks() {
        List<Token> tokens = Token.getTokens(
                Locator.createLocatorFromPathComponents("12345", "com", "example", "a", "b", "c"));
        for (Token token : tokens) {
            assertThat(cache.isTokenCurrent(token), is(false));
            cache.setTokenCurrent(token);
            assertThat(cache.isTokenCurrent(token), is(true));
        }
        // Five tokens expected: com, com.example, com.example.a, com.example.a.b, and com.example.a.b.c
        // The tenant isn't considered a separate token, but instead is part of each token's identifier.
        assertThat(cache.getCurrentLocatorCount(), equalTo(5L));
    }

    @Test
    public void resetWorks() {
        List<Token> tokens = Token.getTokens(Locator.createLocatorFromDbKey("12345.a.b.c"));
        for (Token token : tokens) {
            cache.setTokenCurrent(token);
            assertThat(cache.isTokenCurrent(token), is(true));
        }
        assertThat(cache.getCurrentLocatorCount(), equalTo(3L));
        cache.resetCache();
        assertThat(cache.getCurrentLocatorCount(), equalTo(0L));
        for (Token token : tokens) {
            assertThat(cache.isTokenCurrent(token), is(false));
        }
    }

    @Test
    public void expiresStuff() throws Exception {
        // Given several random tokens
        Random r = new Random();
        List<Token> tokens = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Locator locator = Locator.createLocatorFromDbKey(
                    "tenant.l1-" + r.nextInt(10) + ".l2-" + r.nextInt(10) + ".leaf");
            tokens.addAll(Token.getTokens(locator));
        }
        // And a cache with a fairly short timeout
        TokenCache cache = TokenCache.getInstance(100, MILLISECONDS);
        // When I insert all the tokens into the cache
        tokens.forEach(cache::setTokenCurrent);
        // Then the cache reflects all of the entries
        tokens.forEach(token -> assertThat(cache.isTokenCurrent(token), is(true)));
        // And after the timeout, all of them are no longer cached
        Thread.sleep(101);
        tokens.forEach(token -> assertThat(cache.isTokenCurrent(token), is(false)));
    }

    @Test
    // Repeat in order to exercise multiple values for the various random settings in the test, providing greater
    // overall coverage.
    @Repeating(repetition = 25)
    public void handlesRandomConcurrencyLoad() {
        Random r = new Random();
        ConcurrentLinkedQueue<Token> tokensAddedToCache = new ConcurrentLinkedQueue<>();
        int concurrency = 2 + r.nextInt(20);
        int executions = 1 + r.nextInt(1000);
        System.out.println("Test concurrency with " + concurrency + " threads and " + executions + " executions");
        doConcurrently(concurrency, executions, () -> {
            Locator locator = Locator.createLocatorFromDbKey(
                    "tenant.l1-" + r.nextInt(10) + ".l2-" + r.nextInt(10) + ".leaf");
            for (Token token : Token.getTokens(locator)) {
                if (!cache.isTokenCurrent(token)) {
                    cache.setTokenCurrent(token);
                }
                // Remember what we did
                tokensAddedToCache.offer(token);
            }
        });
        // Sanity check our data - each locator breaks into three tokens
        assertThat(tokensAddedToCache.size(), equalTo(executions * 3));
        // Now verify that everything we set is remembered
        for (Token token : tokensAddedToCache) {
            assertThat(cache.isTokenCurrent(token), is(true));
        }
    }

    @Test
    // DO NOT synchronize this cache. While not as heavily used as the LocatorCache (see the similar test there),
    // there's no reason for synchronization, since this is built on a concurrent cache already!
    public void mustNotBeSynchronized() throws IOException {
        Path path = Paths.get("src/main/java/com/rackspacecloud/blueflood/cache/TokenCache.java");
        for (String line : Files.readAllLines(path)) {
            if (line.contains("synchronized")) {
                fail("Found 'synchronized' in line:\n" + line);
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
