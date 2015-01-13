package com.rackspacecloud.blueflood.service.udp.functions;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.service.udp.UDPMetricSerialization;
import com.rackspacecloud.blueflood.types.Metric;
import io.netty.channel.socket.DatagramPacket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Deserializes a UDP packet and releases its buffer.
 */
public class DeserializeAndReleaseFunc extends FunctionWithThreadPool<DatagramPacket, ListenableFuture<Collection<Metric>>> {

    private static final Collection<Metric> EMPTY = Collections.unmodifiableCollection(new ArrayList<Metric>());
    
    public DeserializeAndReleaseFunc(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }
    
    @Override
    public ListenableFuture<Collection<Metric>> apply(final DatagramPacket input) throws Exception {
        
        // do it on the threadpool.
        return getThreadPool().submit(new Callable<Collection<Metric>>() {
            public Collection<Metric> call() throws Exception {
                try {
                    
                    // In this case, we use a custom serialization specified in UDPMetricSerialization.  But you get the point:
                    // this part is up to you.
                    Collection<Metric> metrics = UDPMetricSerialization.fromDatagram(input);
                    
                    // netty idiom.
                    input.content().release();
                    return metrics;
                } catch (Exception ex) {
                    getLogger().error("Dropping packet: " + ex.getMessage());
                    return EMPTY;
                }
            }
        });
    }
    
    
}
