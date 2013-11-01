package com.rackspacecloud.blueflood.inputs.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.Metric;

import org.junit.Test;
import static org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;


public class DiscoveryWriterTest {
    @Test
    public void testProcessor() throws Exception {
        DiscoveryWriter discWriter =
                new DiscoveryWriter(new ThreadPoolBuilder()
                        .withName("Metric Discovery Writing")
                        .withCorePoolSize(10)
                        .withMaxPoolSize(10)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build());

        DiscoveryIO discovererA = mock(DiscoveryIO.class);
        DiscoveryIO discovererB = mock(DiscoveryIO.class);
        discWriter.registerIO(discovererA);
        discWriter.registerIO(discovererB);

        // create fake metrics to test
        List<List<Metric>> testdata = new ArrayList<List<Metric>>();
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));

        ListenableFuture<List<Boolean>> result = discWriter.processMetrics(testdata);
        // wait until DiscoveryWriter finishes processing all the fake metrics
        while (!result.isDone()) {
           Thread.sleep(500);
        }

        // verify the insertDiscovery method on all implementors of
        // DiscoveryIO has been called on all metrics
        for (List<Metric> metrics : testdata) {
            verify(discovererA).insertDiscovery(metrics);
            verify(discovererB).insertDiscovery(metrics);
        }
    }
}
