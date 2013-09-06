package com.rackspacecloud.blueflood.service.udp.functions;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.service.udp.UDPMetricSerialization;
import com.rackspacecloud.blueflood.types.Metric;
import io.netty.channel.socket.DatagramPacket;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Deserializes a UDP packet and releases its buffer.
 */
public class DeserializeAndReleaseFunc extends AsyncFunctionWithThreadPool<DatagramPacket, Metric> {

    public DeserializeAndReleaseFunc(ThreadPoolExecutor threadPool) {
        super(threadPool);
    }
    
    @Override
    public ListenableFuture<Metric> apply(final DatagramPacket input) throws Exception {
        
        // do it on the threadpool.
        return getThreadPool().submit(new Callable<Metric>() {
            public Metric call() throws Exception {
                try {
                    
                    // In this case, we use a custom serialization specified in UDPMetricSerialization.  But you get the point:
                    // this part is up to you.
                    Metric metric = UDPMetricSerialization.fromDatagram(input);
                    
                    // netty idiom.
                    input.content().release();
                    return metric;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            }
        });
    }
    
    
}
