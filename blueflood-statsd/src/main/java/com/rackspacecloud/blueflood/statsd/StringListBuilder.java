package com.rackspacecloud.blueflood.statsd;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Constructs strings from buffers. Keep in mind that some characters are multiple bytes and may span two buffers.
 * For that reason `new String(buf)` is insufficient. We've got to do this the hard way.
 * 
 * Ideally, this should be done with zero copies.  Areas that can be improved have been tagged with 'COPY_ALERT'.
 * When things are optimized we may be able to get away using a CharSequence implementation that composes the list of
 * ByteBuf objects.
 */
public class StringListBuilder extends AsyncFunctionWithThreadPool<List<ByteBuf>, List<CharSequence>> {
    
    private static final Charset CHARSET = Charsets.UTF_8;
    private static final byte EOL = (byte)'\n';
    
    public StringListBuilder(ThreadPoolExecutor executor) {
        super(executor);
    }

    @Override
    public ListenableFuture<List<CharSequence>> apply(final List<ByteBuf> input) throws Exception {
        return getThreadPool().submit(new Callable<List<CharSequence>>() {
            @Override
            public List<CharSequence> call() throws Exception {
                return StringListBuilder.buildStrings(input);
            }
        });
    }
    
    /**
     * Try to build a list of strings from a list of byte arrays with as few buffer copies as possible.  This can be
     * improved by implementing a UTF-8 state machine byte reader and then iterating over the ByteBuf objects.
     */
    public static List<CharSequence> buildStrings(List<ByteBuf> input) {
        final List<CharSequence> strings = new ArrayList<CharSequence>();
        final BytesBuilder builder = new BytesBuilder();
        for (ByteBuf bb : input) {
            bb.forEachByte(new ByteBufProcessor() {
                @Override
                public boolean process(byte value) throws Exception {
                    if (value == EOL) {
                        strings.add(new String(builder.toArray(), CHARSET));
                        builder.clear();
                    } else {
                        builder.add(value);
                    }
                    return true;
                }
            });
            
            bb.release();
            
        }
        
        if (builder.size() > 0)
            strings.add(new String(builder.toArray(), CHARSET));

        return strings;
    }

    private static class BytesBuilder {
        private LinkedList<Byte> bytes = new LinkedList<Byte>();
        
        public void add(byte b) {
            bytes.add(b);
        }
        
        public byte[] toArray() {
            // COPY ALERT.
            byte[] arr = new byte[bytes.size()];
            int index = 0;
            for (Byte b : bytes)
                arr[index++] = b;
            return arr;
        }
        
        public void clear() {
            bytes.clear();
        }
        
        public int size() {
            return bytes.size();
        }
    }
}
