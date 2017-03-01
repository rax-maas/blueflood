package com.rackspacecloud.blueflood.inputs.processors;


import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.cache.TokenCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.TokenDiscoveryIO;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TokenDiscoveryWriterTest {

    @Before
    public void setUp() throws Exception {
        LocatorCache.getInstance().resetCache();
        TokenCache.getInstance().resetCache();
    }

    @Test
    public void testGenerateAndConsolidateTokens() {

        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        List<List<IMetric>> batch = new ArrayList<>();
        batch.add(Arrays.asList(new Metric(locator, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));

        Set<Token> tokens = TokenDiscoveryWriter.getUniqueTokens(TokenDiscoveryWriter.getLocators(batch));

        assertEquals("Invalid number of tokens", locator.getMetricName().split(Locator.METRIC_TOKEN_SEPARATOR_REGEX).length, tokens.size());

    }

    @Test
    public void testGenerateAndConsolidateTokensMultipleMetrics() {

        Locator locator1 = Locator.createLocatorFromPathComponents("111111", "a.b.c.d");
        Locator locator2 = Locator.createLocatorFromPathComponents("111111", "a.b.c.e");

        List<List<IMetric>> batch = new ArrayList<>();
        batch.add(Arrays.asList(new Metric(locator1, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));
        batch.add(Arrays.asList(new Metric(locator2, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));

        Set<Token> tokens = TokenDiscoveryWriter.getUniqueTokens(TokenDiscoveryWriter.getLocators(batch));

        assertEquals("Invalid number of tokens", 5, tokens.size());

    }

    @Test
    public void testProcessTokensMultipleLocatorsBeingIndexed() throws Exception {

        TokenDiscoveryWriter tokenWriter =
                new TokenDiscoveryWriter(new ThreadPoolBuilder()
                                            .withName("Metric Token Discovery Writing")
                                            .withCorePoolSize(10)
                                            .withMaxPoolSize(10)
                                            .withUnboundedQueue()
                                            .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                                            .build());

        TokenDiscoveryIO discovererA = mock(TokenDiscoveryIO.class);
        TokenDiscoveryIO discovererB = mock(TokenDiscoveryIO.class);

        tokenWriter.registerIO(discovererA);
        tokenWriter.registerIO(discovererB);

        List<List<IMetric>> batch = new ArrayList<>();
        Locator locator1 = Locator.createLocatorFromPathComponents("111111", "a.b.c.d");
        batch.add(Arrays.asList(new Metric(locator1, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));

        String[] tokens = locator1.getMetricName().split(Locator.METRIC_TOKEN_SEPARATOR_REGEX);
        List<Token> expectedTokens = IntStream.range(0, tokens.length)
                                              .mapToObj(x -> new Token(locator1, tokens, x)).collect(toList());

        boolean success = tokenWriter.processTokens(batch).get();
        assertTrue(success);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(discovererA, times(1)).insertDiscovery(captor.capture());
        List<Token> actualTokensA = captor.getValue();

        assertEquals("Unexpected number of tokens", 4, actualTokensA.size());
        assertTrue(actualTokensA.containsAll(expectedTokens) && expectedTokens.containsAll(actualTokensA));

        verify(discovererB, times(1)).insertDiscovery(captor.capture());
        List<Token> actualTokensB = captor.getValue();

        assertEquals("Unexpected number of tokens", 4, actualTokensB.size());
        assertTrue(actualTokensB.containsAll(expectedTokens) && expectedTokens.containsAll(actualTokensB));
    }

    @Test
    public void testProcessTokensSingleLocatorBeingIndexed() throws Exception {

        TokenDiscoveryWriter tokenWriter =
                new TokenDiscoveryWriter(new ThreadPoolBuilder()
                                                 .withName("Metric Token Discovery Writing")
                                                 .withCorePoolSize(10)
                                                 .withMaxPoolSize(10)
                                                 .withUnboundedQueue()
                                                 .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                                                 .build());

        TokenDiscoveryIO discovererA = mock(TokenDiscoveryIO.class);
        tokenWriter.registerIO(discovererA);

        List<List<IMetric>> batch = new ArrayList<>();
        Locator locator1 = Locator.createLocatorFromPathComponents("111111", "a.b.c.d");
        batch.add(Arrays.asList(new Metric(locator1, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));

        tokenWriter.processTokens(batch).get();
        tokenWriter.processTokens(batch).get(); //no tokens should be indexed with this call as they are already indexed and stored in cache

        verify(discovererA, times(1)).insertDiscovery(anyList());
    }

    @Test
    public void testProcessTokensVerifyingCache() throws Exception {

        TokenDiscoveryWriter tokenWriter =
                new TokenDiscoveryWriter(new ThreadPoolBuilder()
                                                 .withName("Metric Token Discovery Writing")
                                                 .withCorePoolSize(10)
                                                 .withMaxPoolSize(10)
                                                 .withUnboundedQueue()
                                                 .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                                                 .build());

        TokenDiscoveryIO discovererA = mock(TokenDiscoveryIO.class);
        tokenWriter.registerIO(discovererA);

        List<List<IMetric>> batch = new ArrayList<>();
        Locator locator1 = Locator.createLocatorFromPathComponents("111111", "a.b.c.d");
        Locator locator2 = Locator.createLocatorFromPathComponents("111111", "a.b.c.e");
        batch.add(Arrays.asList(new Metric(locator1, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));
        batch.add(Arrays.asList(new Metric(locator2, 1, 1L, new TimeValue(5, TimeUnit.SECONDS), "")));

        String[] tokens1 = locator1.getMetricName().split(Locator.METRIC_TOKEN_SEPARATOR_REGEX);
        List<Token> expectedTokens = IntStream.range(0, tokens1.length)
                                              .mapToObj(x -> new Token(locator1, tokens1, x)).collect(toList());
        String[] tokens2 = locator2.getMetricName().split(Locator.METRIC_TOKEN_SEPARATOR_REGEX);
        expectedTokens.add( new Token(locator2, tokens2, 3)); //notice that there is only new expected token from the second locator.

        tokenWriter.processTokens(batch).get();

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(discovererA, times(1)).insertDiscovery(captor.capture());

        List<Token> actualTokensA = captor.getValue();
        assertEquals("Unexpected number of tokens", 5, actualTokensA.size());
        assertTrue(actualTokensA.containsAll(expectedTokens) && expectedTokens.containsAll(actualTokensA));
    }

}
