package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.cache.TokenCache;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ElasticTokensIOTest {

    protected ElasticTokensIO elasticTokensIO;
    private ElasticsearchRestHelper mockElasticsearchRestHelper;

    @Before
    public void setup() throws IOException {
        elasticTokensIO = new ElasticTokensIO();
        mockElasticsearchRestHelper = mock(ElasticsearchRestHelper.class);
        elasticTokensIO.setElasticsearchRestHelper(mockElasticsearchRestHelper);
        TokenCache.getInstance().resetCache();
    }

    @Test
    public void testGetIndexesToSearch() throws IOException {
        String[] indices = elasticTokensIO.getIndexesToSearch();
        assertEquals(1, indices.length);
        assertEquals("metric_tokens", indices[0]);
    }

    @Test
    public void testCreateSingleRequest_WithNullMetricName() throws IOException {
        final String TENANT_A = "12345";
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_A);
        assertEquals(0, Token.getTokens(locator).size());
    }

    @Test
    public void testRegexLevel0() {
        List<String> terms = Arrays.asList("", "foo", "bar", "b", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(3, matchingTerms.size());
        assertEquals("foo", matchingTerms.get(0));
        assertEquals("bar", matchingTerms.get(1));
        assertEquals("b", matchingTerms.get(2));
    }

    @Test
    public void testRegexLevel0WildCard() {
        List<String> terms = Arrays.asList("", "foo", "bar", "baz", "b", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("b*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(3, matchingTerms.size());
        assertEquals("bar", matchingTerms.get(0));
        assertEquals("baz", matchingTerms.get(1));
        assertEquals("b", matchingTerms.get(2));
    }

    @Test
    public void testRegexLevel1() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.b", "foo.xxx", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("foo.*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(3, matchingTerms.size());
        assertEquals("foo.bar", matchingTerms.get(0));
        assertEquals("foo.b", matchingTerms.get(1));
        assertEquals("foo.xxx", matchingTerms.get(2));
    }

    @Test
    public void testRegexLevel1WildCard() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.b", "foo.xxx", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("foo.b*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(2, matchingTerms.size());
        assertEquals("foo.bar", matchingTerms.get(0));
        assertEquals("foo.b", matchingTerms.get(1));
    }

    @Test
    public void testRegexLevel2() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.qux", "foo.bar.baz.qux.quux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("foo.bar.*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(1, matchingTerms.size());
        assertEquals("foo.bar.baz", matchingTerms.get(0));
    }

    @Test
    public void testRegexLevel3() {
        List<String> terms = Arrays.asList("foo", "bar", "baz", "foo.bar", "foo.bar.baz", "foo.bar.baz.qux", "foo.bar.baz.qux.quux");

        List<String> matchingTerms = new ArrayList<>();
        Pattern patternToGet2Levels = Pattern.compile(elasticTokensIO.getRegexToHandleTokens(new GlobPattern("foo.bar.baz.*")));
        for (String term: terms) {
            Matcher matcher = patternToGet2Levels.matcher(term);
            if (matcher.matches()) {
                matchingTerms.add(term);
            }
        }

        assertEquals(1, matchingTerms.size());
        assertEquals("foo.bar.baz.qux", matchingTerms.get(0));
    }

    @Test
    public void testEmptyInputIsOkay() throws Exception {
        elasticTokensIO.insertDiscovery(Collections.emptyList());
        verify(mockElasticsearchRestHelper, never()).indexTokens(any());
    }

    @Test
    public void testOnlyUniqueTokensAreIndexed() throws Exception {
        // Given a couple of locators with overlapping tokens
        Locator locator1 = Locator.createLocatorFromPathComponents("1234", "a.b.c.d");
        Locator locator2 = Locator.createLocatorFromPathComponents("1234", "a.b.c.e");

        // When I insert metrics with those locators
        elasticTokensIO.insertDiscovery(Arrays.asList(randomMetric(locator1), randomMetric(locator2)));

        // Then all tokens for both locators are indexed
        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockElasticsearchRestHelper).indexTokens(listCaptor.capture());
        List<Token> actualTokens = listCaptor.getValue();
        assertThat(actualTokens, hasItem(new Token(locator1, new String[]{"a", "b", "c", "d"}, 0)));
        assertThat(actualTokens, hasItem(new Token(locator1, new String[]{"a", "b", "c", "d"}, 1)));
        assertThat(actualTokens, hasItem(new Token(locator1, new String[]{"a", "b", "c", "d"}, 2)));
        assertThat(actualTokens, hasItem(new Token(locator1, new String[]{"a", "b", "c", "d"}, 3)));
        assertThat(actualTokens, hasItem(new Token(locator2, new String[]{"a", "b", "c", "e"}, 0)));
        assertThat(actualTokens, hasItem(new Token(locator2, new String[]{"a", "b", "c", "e"}, 1)));
        assertThat(actualTokens, hasItem(new Token(locator2, new String[]{"a", "b", "c", "e"}, 2)));
        assertThat(actualTokens, hasItem(new Token(locator2, new String[]{"a", "b", "c", "e"}, 3)));

        // And only the number of non-duplicate tokens are indexed
        // (effectively, tokens 'a', 'b', 'c', 'd', and 'e' are the unique tokens)
        // (or put another way: 'a', 'a.b', 'a.b.c', 'a.b.c.d', and 'a.b.c.e'))
        assertThat(actualTokens, hasSize(5));
    }

    @Test
    public void testNonLeafTokensAreFilteredByCache() throws Exception {
        // Given a couple of distinct locators
        Locator locator1 = Locator.createLocatorFromPathComponents("1234", "a.b.c.d");
        Locator locator2 = Locator.createLocatorFromPathComponents("4321", "z.y.x.w");

        // When I insert metrics with those locators
        elasticTokensIO.insertDiscovery(Arrays.asList(randomMetric(locator1), randomMetric(locator2)));

        // Then all tokens for both locators are indexed
        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockElasticsearchRestHelper).indexTokens(listCaptor.capture());
        List<Token> actualTokens = listCaptor.getValue();
        assertThat(actualTokens, hasSize(8));

        // When I insert new metrics with the same locators
        elasticTokensIO.insertDiscovery(Arrays.asList(randomMetric(locator1), randomMetric(locator2)));

        // Then all non-leaf tokens are skipped because they're in the cache as "already seen"
        // (We assume the leaf node, corresponding to the entire locator value, is guarded by the LocatorCache in
        // DiscoveryWriter. Caching it again here would be redundant and double cache memory usage.)
        verify(mockElasticsearchRestHelper, times(2)).indexTokens(listCaptor.capture());
        actualTokens = listCaptor.getValue();
        assertThat(actualTokens, hasItem(new Token(locator1, new String[]{"a", "b", "c", "d"}, 3)));
        assertThat(actualTokens, hasItem(new Token(locator2, new String[]{"z", "y", "x", "w"}, 3)));
        assertThat(actualTokens, hasSize(2));
    }

    private final Random random = new Random();
    private IMetric randomMetric(Locator locator) {
        return new Metric(locator, random.nextInt(1000), random.nextInt(1000), new TimeValue(10, TimeUnit.SECONDS), "test unit");
    }
}
