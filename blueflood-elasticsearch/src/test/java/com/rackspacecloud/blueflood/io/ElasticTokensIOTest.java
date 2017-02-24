package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.GlobPattern;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class ElasticTokensIOTest {

    protected ElasticTokensIO elasticTokensIO;

    @Before
    public void setup() throws IOException {
        elasticTokensIO = new ElasticTokensIO();
    }

    @Test
    public void testCreateSingleRequest_WithNullMetricName() throws IOException {
        final String TENANT_A = "12345";
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_A);
        assertEquals(0, Token.getTokens(locator).size());
    }

    @Test
    public void testCreateSingleRequest() throws IOException {
        final String TENANT_A = "12345";
        final String METRIC_NAME = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(TENANT_A, METRIC_NAME);

        String[] expectedTokens = new String[] {"a", "b", "c", "d"};

        String[] expectedPaths = new String[] {
                "",
                "a",
                "a.b",
                "a.b.c"};

        String[] expectedIds = new String[] {
                TENANT_A + ":" + "a",
                TENANT_A + ":" + "a.b",
                TENANT_A + ":" + "a.b.c",
                TENANT_A + ":" + "a.b.c.d:$"};

        boolean[] expectedIsLeaf = new boolean[]{false, false, false, true};

        int count = 0;
        for (Token token: Token.getTokens(locator)) {
            IndexRequestBuilder builder = elasticTokensIO.createSingleRequest(token);
            Assert.assertNotNull(builder);
            assertEquals("invalid document id", expectedIds[count], builder.request().id());
            final String expectedIndex =
                    "index {" +
                            "[" + ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE + "]" +
                            "[" + ElasticTokensIO.ES_DOCUMENT_TYPE + "]" +
                            "["+ expectedIds[count] + "], " +
                            "source[{" +
                            "\"token\":\"" + expectedTokens[count] + "\"," +
                            "\"parent\":\"" + expectedPaths[count] + "\"," +
                            "\"isLeaf\":" + expectedIsLeaf[count] + "," +
                            "\"tenantId\":\"" + TENANT_A + "\"" +
                            "}]}";
            assertEquals("Invalid Level:" + count, expectedIndex, builder.request().toString());
            assertEquals(builder.request().routing(), TENANT_A);
            count++;
        }
    }

    @Test
    public void testRegexLevel0() {
        List<String> terms = Arrays.asList("", "foo", "bar", "b", "foo.bar", "foo.bar.baz", "foo.bar.baz.aux");

        List<String> matchingTerms = new ArrayList<String>();
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

        List<String> matchingTerms = new ArrayList<String>();
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

        List<String> matchingTerms = new ArrayList<String>();
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

        List<String> matchingTerms = new ArrayList<String>();
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

        List<String> matchingTerms = new ArrayList<String>();
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

        List<String> matchingTerms = new ArrayList<String>();
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
