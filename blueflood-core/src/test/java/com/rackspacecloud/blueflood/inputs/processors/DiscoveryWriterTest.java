package com.rackspacecloud.blueflood.inputs.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


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
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        testdata.add(mock(List.class));
        
        List<IMetric> flatTestData = new ArrayList<IMetric>();
        for (List<IMetric> list : testdata) {
            if (list.size() == 0) continue;
            for (IMetric m : list) {
                flatTestData.add(m);
            }
        }

        
        ListenableFuture<Boolean> result = discWriter.processMetrics(testdata);
        // wait until DiscoveryWriter finishes processing all the fake metrics
        Assert.assertTrue(result.get());

        // verify the insertDiscovery method on all implementors of
        // DiscoveryIO has been called on all metrics
        verify(discovererA).insertDiscovery(flatTestData);
        verify(discovererB).insertDiscovery(flatTestData);
    }
}
