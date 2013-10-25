package com.rackspacecloud.blueflood.inputs.processors;

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.Metric;

import org.junit.Test;
import static org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;


public class DiscoveryWriterTest {
    private static int WRITE_THREADS = 50;
    @Test
    public void testProcessor() throws Exception {
        DiscoveryIO discovererA = mock(DiscoveryIO.class);
        DiscoveryIO discovererB = mock(DiscoveryIO.class);
        DiscoveryWriter discWriter =
                new DiscoveryWriter(new ThreadPoolBuilder()
                        .withName("Metric Discovery Writing")
                        .withCorePoolSize(WRITE_THREADS)
                        .withMaxPoolSize(WRITE_THREADS)
                        .withUnboundedQueue()
                        .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                        .build());
        discWriter.registerIO(discovererA);
        discWriter.registerIO(discovererB);

        List<List<Metric>> testdata = new ArrayList<List<Metric>>();
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));

        discWriter.apply(testdata);

        for (List<Metric> metrics : testdata) {
            verify(discovererA).insertDiscovery(metrics);
            verify(discovererB).insertDiscovery(metrics);
        }
    }
}
