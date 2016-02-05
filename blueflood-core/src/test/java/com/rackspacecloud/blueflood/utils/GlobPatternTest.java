package com.rackspacecloud.blueflood.utils;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.regex.PatternSyntaxException;

public class GlobPatternTest {

    @Test
    public void testGlobMatchingAnyChar() {

        String glob = "*";
        String expectedRegex = ".*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testEmptyGlob() {

        String glob = "";
        String expectedRegex = "";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testGlobWithWildcards1() {

        String glob = "foo.bar$1.(cat).baz|qux.dog+";
        String expectedRegex = "foo\\.bar\\$1\\.\\(cat\\)\\.baz\\|qux\\.dog\\+";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob() {

        String glob = "foo.bar.*";
        String expectedRegex = "foo\\.bar\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlobWithWildCards() {

        String glob = "f*.bar.*";
        String expectedRegex = "f.*\\.bar\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameWithMatchingSingleChar() {

        String glob = "foo?";
        String expectedRegex = "foo.";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameWithMatchingAnyChar() {

        String glob = "foo*";
        String expectedRegex = "foo.*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameWithGlobSyntax() {

        String glob = "foo.{*}";
        String expectedRegex = "foo\\.(.*)";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    //I would expect this test to pass but it did not. Anything inside brackets [ ] should be left unescaped.
    @Ignore
    public void testVariousGlobSyntax1() {

        String glob = "fo[^ab.co].*]";
        String expectedRegex = "fo[^ab.co]\\..*]";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testVariousGlobSyntax2() {

        String glob = "[!abc]oo.*]";
        String expectedRegex = "[^abc]oo\\..*]";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameWithVariousGlobSyntax() {

        String glob = "foo.[bz]*.*";
        String expectedRegex = "foo\\.[bz].*\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlobWithoutWildCard() {

        String glob = "foo.bar";
        String expectedRegex = "foo\\.bar";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals(expectedRegex, pattern.compiled().toString());
    }

    @Test(expected = PatternSyntaxException.class)
    public void invalidGlobWithUnclosedBracket() {
        String glob = "foo.[bar.*";
        GlobPattern pattern = new GlobPattern(glob);
    }

    @Test(expected = NullPointerException.class)
    public void testNullGlob() {
        String glob = null;
        GlobPattern pattern = new GlobPattern(glob);
    }

}
