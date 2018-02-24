package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class TokenTest {

    @Test (expected = IllegalArgumentException.class)
    public void createTokenNullTokens() {
        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        new Token(locator, null, 0);
    }

    @Test (expected = IllegalArgumentException.class)
    public void createTokenEmptyTokens() {
        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        new Token(locator, new String[0], 0);
    }

    @Test (expected = IllegalArgumentException.class)
    public void createTokenInvalidLevel1() {
        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        new Token(locator, metricName.split(Locator.METRIC_TOKEN_SEPARATOR_REGEX), -1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void createTokenInvalidLevel2() {
        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        new Token(locator, metricName.split(Locator.METRIC_TOKEN_SEPARATOR_REGEX), 4);
    }

    @Test
    public void testGetTokensHappyCase() {

        String tenantID = "111111";
        String metricName = "a.b.c.d";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        String[] expectedTokens = new String[] {"a", "b", "c", "d"};

        String[] expectedParents = new String[] {
                "",
                "a",
                "a.b",
                "a.b.c"};

        String[] expectedIds = new String[] {
                tenantID + ":" + "a",
                tenantID + ":" + "a.b",
                tenantID + ":" + "a.b.c",
                tenantID + ":" + "a.b.c.d:$"};

        List<Token> tokens = Token.getTokens(locator);

        verifyTokenInfos(tenantID, expectedTokens, expectedParents, expectedIds, tokens);
    }

    @Test
    public void testGetTokensForMetricWithOneToken() {

        String tenantID = "111111";
        String metricName = "a";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        String[] expectedTokens = new String[] {"a"};
        String[] expectedParents = new String[] {""};
        String[] expectedIds = new String[] {tenantID + ":" + "a:$"};

        List<Token> tokens = Token.getTokens(locator);
        verifyTokenInfos(tenantID, expectedTokens, expectedParents, expectedIds, tokens);
    }

    @Test
    public void testGetTokensWithEmptyTokenInBetween() {

        String tenantID = "111111";
        String metricName = "ingest00.HeaderNormalization.header-normalization..*_GET.count";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);


        String[] expectedTokens = new String[] {"ingest00", "HeaderNormalization", "header-normalization", "", "*_GET", "count"};

        String[] expectedParents = new String[] {
                "",
                "ingest00",
                "ingest00.HeaderNormalization",
                "ingest00.HeaderNormalization.header-normalization",
                "ingest00.HeaderNormalization.header-normalization.",
                "ingest00.HeaderNormalization.header-normalization..*_GET"};

        String[] expectedIds = new String[] {
                tenantID + ":" + "ingest00",
                tenantID + ":" + "ingest00.HeaderNormalization",
                tenantID + ":" + "ingest00.HeaderNormalization.header-normalization",
                tenantID + ":" + "ingest00.HeaderNormalization.header-normalization.",
                tenantID + ":" + "ingest00.HeaderNormalization.header-normalization..*_GET",
                tenantID + ":" + "ingest00.HeaderNormalization.header-normalization..*_GET.count:$"};

        List<Token> tokens = Token.getTokens(locator);

        verifyTokenInfos(tenantID, expectedTokens, expectedParents, expectedIds, tokens);
    }

    @Test
    public void testGetTokensForMetricNoTokens() {

        String tenantID = "111111";
        String metricName = "";
        Locator locator = Locator.createLocatorFromPathComponents(tenantID, metricName);

        List<Token> tokens = Token.getTokens(locator);
        assertEquals("Total number of tokens invalid", 0, tokens.size());
    }


    private void verifyTokenInfos(String tenantID, String[] expectedTokens, String[] expectedParents,
                                  String[] expectedIds, List<Token> actualTokenInfos) {

        assertEquals("Total number of tokens invalid", expectedTokens.length, actualTokenInfos.size());

        actualTokenInfos.stream().forEach(x -> assertEquals(tenantID, x.getLocator().getTenantId()));


        String[] actualTokens = actualTokenInfos.stream()
                                                .map(Token::getToken)
                                                .collect(toList()).toArray(new String[0]);

        assertArrayEquals("Tokens mismatch", expectedTokens, actualTokens);

        String[] actualPaths = actualTokenInfos.stream()
                                               .map(Token::getParent)
                                               .collect(toList()).toArray(new String[0]);

        assertArrayEquals("Token parents mismatch", expectedParents, actualPaths);

        String[] actualIds = actualTokenInfos.stream()
                                             .map(Token::getId)
                                             .collect(toList()).toArray(new String[0]);

        assertArrayEquals("Token Ids mismatch", expectedIds, actualIds);
    }
}
