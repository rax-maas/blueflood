package com.rackspacecloud.blueflood.statsd;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.UnpooledByteBufAllocator;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StringListBuilderTest {
    
    @Test
    public void testStrToByteBufWorks() {
        final String str = "abcdefghij";
        final ByteBuf buf = StringListBuilderTest.strToBuf(str);
        final AtomicInteger counter = new AtomicInteger(0);
        buf.forEachByte(new ByteBufProcessor() {
            @Override
            public boolean process(byte value) throws Exception {
                counter.incrementAndGet();
                return true;
            }
        });
        Assert.assertEquals(10, str.length());
        Assert.assertEquals(str.length(), counter.get());
    }
    
    private static ByteBuf strToBuf(String str) {
        byte[] buf = str.getBytes(Charsets.UTF_8);
        return arrToBuf(buf);
    }
    
    private static ByteBuf arrToBuf(byte[] buf) {
        ByteBuf bb0 = UnpooledByteBufAllocator.DEFAULT.buffer(buf.length);
        bb0 = bb0.writeBytes(buf);
        
        Assert.assertTrue(buf.length > 0);
        Assert.assertTrue(bb0.isReadable());
        Assert.assertEquals(buf.length, bb0.readableBytes());
        
        return bb0;
    }
    
    @Test
    public void testSingleBuffer() {
        List<CharSequence> strings;
        Iterator<CharSequence> it;
        
        final ByteBuf foobarBytes = strToBuf("foobar\n");
        strings = StringListBuilder.buildStrings(new ArrayList<ByteBuf>() {{
            add(foobarBytes);
        }});
        it = strings.iterator();
        Assert.assertEquals(1, strings.size());
        Assert.assertEquals("foobar", it.next());
        
        // string is "ⅅusbabek"
        final ByteBuf fancyDBytes = strToBuf("\u2145usbabek\n");
        strings = StringListBuilder.buildStrings(new ArrayList<ByteBuf>() {{
            add(fancyDBytes);
        }});
        it = strings.iterator();
        Assert.assertEquals(1, strings.size());
        Assert.assertEquals("\u2145usbabek", it.next());
        
        final ByteBuf multipleStrings = strToBuf("foobar\n\u2145usbabek\n");
        strings = StringListBuilder.buildStrings(new ArrayList<ByteBuf>() {{
            add(multipleStrings);
        }});
        it = strings.iterator();
        Assert.assertEquals(2, strings.size());
        Assert.assertEquals("foobar", it.next());
        Assert.assertEquals("\u2145usbabek", it.next());
    }
    
    @Test
    public void testNoEndOfLine() {
        final ByteBuf unterminated = strToBuf("foobar");
        List<CharSequence> strings = StringListBuilder.buildStrings(new ArrayList<ByteBuf>() {{
            add(unterminated);
        }});
        Assert.assertEquals(1, strings.size());
    }
    
    @Test
    public void testAsciiMultipleBuffersNoSplits() {
        final ByteBuf buf0 = strToBuf("this\nis\na\ntest\n");
        final ByteBuf buf1 = strToBuf("of\nthe\nemergency\nbroadcast\nsystem\n");
        List<ByteBuf> input = new ArrayList<ByteBuf>() {{
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
        final ByteBuf buf0 = strToBuf("this\nis\na\nte");
        final ByteBuf buf1 = strToBuf("st\nof\nthe\nemergency\nbroadcast\nsystem\n");
        List<ByteBuf> input = new ArrayList<ByteBuf>() {{
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
        ByteBuf buf = strToBuf("\u2145");
        ByteBuf expected = arrToBuf(new byte[] { -30, -123, -123 });
        Assert.assertEquals(expected.readableBytes(), buf.readableBytes());
        for (int i = 0; i < expected.readableBytes(); i++)
            Assert.assertEquals(expected.readByte(), buf.readByte());
    }
    
    @Test
    public void testUnicodeSplitIntoSeveralBuffers() {
        // this\nis\na\nⅅusbabek\ntest\n
        // 116,104,105,115,\n,105,115,\n,97,\n,-30,-123, <SPLIT> -123,117,115,98,97,98,101,107\n,116,101,115,116\n
        
        final ByteBuf buf0 = arrToBuf(new byte[] {116,104,105,115,(int)'\n',105,115,(int)'\n',97,(int)'\n',-30,-123}); // this\nis\na\<partial \u2145>
        final ByteBuf buf1 = arrToBuf(new byte[] {-123,117,115,98,97,98,101,107,(int)'\n',116,101,115,116,(int)'\n'}); //<partial \u2145>usbabek\ntest
        List<ByteBuf> input = new ArrayList<ByteBuf>() {{
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
    
    @Test
    public void testMultiplePartialBuffersConsecutively() {
        final ByteBuf buf0 = strToBuf("th");
        final ByteBuf buf1 = strToBuf("is");
        final ByteBuf buf2 = strToBuf("\nworks");

        List<ByteBuf> input = new ArrayList<ByteBuf>() {{
            add(buf0);
            add(buf1);
            add(buf2);
        }};
        List<CharSequence> expected = new ArrayList<CharSequence>() {{
            for (CharSequence s : "this works".split(" ", -1))
                add(s);
        }};

        List<CharSequence> actual = StringListBuilder.buildStrings(input);
        assertSameList(expected, actual);
    }
    
    private static void assertSameList(List<CharSequence> expected, List<CharSequence> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            Assert.assertEquals(expected.get(i), actual.get(i));
    }
}
