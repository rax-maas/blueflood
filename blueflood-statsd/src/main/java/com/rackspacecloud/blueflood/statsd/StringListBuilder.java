package com.rackspacecloud.blueflood.statsd;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import io.netty.buffer.ByteBuf;

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
                
                List<byte[]> buffers = new ArrayList<byte[]>(input.size());
                for (ByteBuf bb : input) {
                    // COPY_ALERT.
                    byte[] raw = new byte[bb.readableBytes()];
                    bb.readBytes(raw);
                    bb.release();
                    buffers.add(raw);
                }
     
                return StringListBuilder.buildStrings(buffers);
            }
        });
    }

    /**
     * Try to build a list of strings from a list of byte arrays with as few buffer copies as possible.  This can be
     * improved by implementing a UTF-8 state machine byte reader and then iterating over the ByteBuf objects.
     */
    public static List<CharSequence> buildStrings(List<byte[]> buffers) {
        List<CharSequence> strings = new LinkedList<CharSequence>();
        byte[] partial = null;
        int partialStart = 0;
        
        for (byte[] raw : buffers) {
            int start = 0;
            int pos = 0;
            
            while (pos < raw.length) {
                if (raw[pos] == EOL) {
                    
                    // see if we need to use partial.
                    if (partial != null) {
                        // will need to construct a temporary buffer and copy. COPY_ALERT
                        byte[] tmp = new byte[(partial.length - partialStart) + pos];
                        System.arraycopy(partial, partialStart, tmp, 0, partial.length - partialStart);
                        System.arraycopy(raw, 0, tmp, partial.length - partialStart, pos);
                        // COPY_ALERT
                        strings.add(new String(tmp, 0, tmp.length, CHARSET));
                        partial = null;
                    } else {
                        // entirely within this buffer.
                        // COPY_ALERT
                        strings.add(new String(raw, start, pos - start, CHARSET));
                    }
                    start = pos + 1;
                }
                pos++;
            }
            
            if (start < pos) {
                // we'll have a partial
                partial = raw;
                partialStart = start;
            } else {
                partial = null;
            }
        }
        return strings;
    }
}
