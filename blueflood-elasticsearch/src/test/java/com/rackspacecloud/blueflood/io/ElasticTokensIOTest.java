package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class ElasticTokensIOTest {

    protected static ElasticTokensIO elasticTokensIO;

    @BeforeClass
    public static void setup() throws IOException {
        elasticTokensIO = new ElasticTokensIO();
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
}
