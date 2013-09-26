package com.rackspacecloud.blueflood.statsd;

import com.google.common.base.Charsets;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StringListBuilderTest {
    
    @Test
    public void testSingleBuffer() {
        List<CharSequence> strings;
        Iterator<CharSequence> it;
        
        final byte[] foobarBytes = "foobar\n".getBytes(Charsets.UTF_8);
        strings = StringListBuilder.buildStrings(new ArrayList<byte[]>() {{
            add(foobarBytes);
        }});
        it = strings.iterator();
        Assert.assertEquals(1, strings.size());
        Assert.assertEquals("foobar", it.next());
        
        // string is "ⅅusbabek"
        final byte[] fancyDBytes = "\u2145usbabek\n".getBytes(Charsets.UTF_8);
        strings = StringListBuilder.buildStrings(new ArrayList<byte[]>() {{
            add(fancyDBytes);
        }});
        it = strings.iterator();
        Assert.assertEquals(1, strings.size());
        Assert.assertEquals("\u2145usbabek", it.next());
        
        final byte[] multipleStrings = "foobar\n\u2145usbabek\n".getBytes(Charsets.UTF_8);
        strings = StringListBuilder.buildStrings(new ArrayList<byte[]>() {{
            add(multipleStrings);
        }});
        it = strings.iterator();
        Assert.assertEquals(2, strings.size());
        Assert.assertEquals("foobar", it.next());
        Assert.assertEquals("\u2145usbabek", it.next());
    }
    
    @Test
    public void testNoEndOfLine() {
        final byte[] unterminated = "foobar".getBytes(Charsets.UTF_8);
        List<CharSequence> strings = StringListBuilder.buildStrings(new ArrayList<byte[]>() {{
            add(unterminated);
        }});
        Assert.assertEquals(0, strings.size());
    }
    
    @Test
    public void testAsciiMultipleBuffersNoSplits() {
        final byte[] buf0 = "this\nis\na\ntest\n".getBytes(Charsets.UTF_8);
        final byte[] buf1 = "of\nthe\nemergency\nbroadcast\nsystem\n".getBytes(Charsets.UTF_8);
        List<byte[]> input = new ArrayList<byte[]>() {{
            add(buf0);
            add(buf1);
        }};
        
        List<String> expected = new ArrayList<String>() {{
            for (String s : "this is a test of the emergency broadcast system".split(" ", -1))
                add(s);
        }};
        List<CharSequence> actual = StringListBuilder.buildStrings(input);
        
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            Assert.assertEquals(expected.get(i), actual.get(i));
    }
    
    @Test
    public void testAsciiMultipleBuffersAcrossSplits() {
        // a word is split across two buffers.
        final byte[] buf0 = "this\nis\na\nte".getBytes(Charsets.UTF_8);
        final byte[] buf1 = "st\nof\nthe\nemergency\nbroadcast\nsystem\n".getBytes(Charsets.UTF_8);
        List<byte[]> input = new ArrayList<byte[]>() {{
            add(buf0);
            add(buf1);
        }};
        
        List<CharSequence> expected = new ArrayList<CharSequence>() {{
            for (CharSequence s : "this is a test of the emergency broadcast system".split(" ", -1))
                add(s);
        }};
        List<CharSequence> actual = StringListBuilder.buildStrings(input);
        Assert.assertEquals(9, expected.size());
        assertSameList(expected, actual);
    }
    
    @Test
    public void testUnicodeByteAssumption() {
        byte[] buf = "\u2145".getBytes(Charsets.UTF_8);
        byte[] expected = new byte[] { -30, -123, -123 };
        Assert.assertEquals(expected.length, buf.length);
        for (int i = 0; i < expected.length; i++)
            Assert.assertEquals(expected[i], buf[i]);
    }
    
    @Test
    public void testUnicodeSplitIntoSeveralBuffers() {
        // this\nis\na\nⅅusbabek\ntest\n
        // 116,104,105,115,\n,105,115,\n,97,\n,-30,-123, <SPLIT> -123,117,115,98,97,98,101,107\n,116,101,115,116\n
        
        final byte[] buf0 = new byte[] {116,104,105,115,(int)'\n',105,115,(int)'\n',97,(int)'\n',-30,-123}; // this\nis\na\<partial \u2145>
        final byte[] buf1 = new byte[] {-123,117,115,98,97,98,101,107,(int)'\n',116,101,115,116,(int)'\n'}; //<partial \u2145>usbabek\ntest
        List<byte[]> input = new ArrayList<byte[]>() {{
            add(buf0);
            add(buf1);
        }};
        
        List<CharSequence> expected = new ArrayList<CharSequence>() {{
            for (CharSequence s : "this is a \u2145usbabek test".split(" ", -1))
                add(s);
        }};
        List<CharSequence> actual = StringListBuilder.buildStrings(input);
        Assert.assertEquals(5, expected.size());
        assertSameList(expected, actual);
    }
    
    private static void assertSameList(List<CharSequence> expected, List<CharSequence> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            Assert.assertEquals(expected.get(i), actual.get(i));
    }
}
