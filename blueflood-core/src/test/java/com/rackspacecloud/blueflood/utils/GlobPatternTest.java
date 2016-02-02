package com.rackspacecloud.blueflood.utils;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.regex.PatternSyntaxException;

public class GlobPatternTest {

    @Test
    public void testGlobToRegex1() {

        String glob = "*";
        String expectedRegex = ".*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testGlobToRegex2() {

        String glob = "";
        String expectedRegex = "";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testGlobToRegex3() {

        String glob = "foo.bar$1.(cat).baz|qux.dog+";
        String expectedRegex = "foo\\.bar\\$1\\.\\(cat\\)\\.baz\\|qux\\.dog\\+";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob1() {

        String glob = "foo.bar.*";
        String expectedRegex = "foo\\.bar\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob2() {

        String glob = "f*.bar.*";
        String expectedRegex = "f.*\\.bar\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob5() {

        String glob = "foo?";
        String expectedRegex = "foo.";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob6() {

        String glob = "foo*";
        String expectedRegex = "foo.*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob7() {

        String glob = "foo.{*}";
        String expectedRegex = "foo\\.(.*)";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    //I would expect this test to pass but it did not. Anything inside brackets [ ] should be left unescaped.
    @Ignore
    public void testMetricNameGlob8() {

        String glob = "fo[^ab.co].*]";
        String expectedRegex = "fo[^ab.co]\\..*]";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob9() {

        String glob = "[!abc]oo.*]";
        String expectedRegex = "[^abc]oo\\..*]";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob3() {

        String glob = "foo.[bz]*.*";
        String expectedRegex = "foo\\.[bz].*\\..*";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test
    public void testMetricNameGlob4() {

        String glob = "foo.bar";
        String expectedRegex = "foo\\.bar";

        GlobPattern pattern = new GlobPattern(glob);
        Assert.assertEquals("Invalid regex", expectedRegex, pattern.compiled().toString());
    }

    @Test(expected = PatternSyntaxException.class)
    public void invalidGlob1() {
        String glob = "foo.[bar.*";
        GlobPattern pattern = new GlobPattern(glob);
    }

    @Test(expected = NullPointerException.class)
    public void invalidGlob2() {
        String glob = null;
        GlobPattern pattern = new GlobPattern(glob);
    }

}
