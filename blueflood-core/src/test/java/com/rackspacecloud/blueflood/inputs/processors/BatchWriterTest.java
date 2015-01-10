package com.rackspacecloud.blueflood.inputs.processors;
import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.IngestionContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.stub;


public class BatchWriterTest {
    private static TimeValue timeout = new TimeValue(5, TimeUnit.SECONDS);

    static List<List<IMetric>> createTestData() {
        List<IMetric> l1 = new ArrayList<IMetric>();
        l1.add(mock(Metric.class));
        l1.add(mock(Metric.class));
        l1.add(mock(Metric.class));
        l1.add(mock(Metric.class));

        List<IMetric> l2 = new ArrayList<IMetric>();
        l2.add(mock(Metric.class));
        l2.add(mock(Metric.class));
        l2.add(mock(Metric.class));
        l2.add(mock(Metric.class));

        List<IMetric> l3 = new ArrayList<IMetric>();
        l3.add(mock(Metric.class));
        l3.add(mock(Metric.class));
        l3.add(mock(Metric.class));
        l3.add(mock(Metric.class));

        List<IMetric> l4 = new ArrayList<IMetric>();
        l4.add(mock(Metric.class));
        l4.add(mock(Metric.class));
        l4.add(mock(Metric.class));
        l4.add(mock(Metric.class));


        // create fake metrics to test
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        testdata.add(l1);
        testdata.add(l2);
        testdata.add(l3);
        testdata.add(l4);

        setupLocators(testdata);
        return testdata;
    }

    static List<List<IMetric>> setupLocators(List<List<IMetric>> testdata) {
        //setup locators
        Integer counter = 0;
        for (List<IMetric> l : testdata) {
            for (IMetric m : l) {
                counter++;
                stub(m.getLocator()).toReturn(Locator.createLocatorFromDbKey(counter.toString()));
                stub(m.getCollectionTime()).toReturn(counter.longValue());
            }
        }
        return testdata;
    }

    @Test
    public void testWriter() throws Exception {
        IMetricsWriter writer = mock(IMetricsWriter.class);
        Counter bufferedMetrics = mock(Counter.class);
        IngestionContext context = mock(IngestionContext.class);
        List<List<IMetric>> testdata = createTestData();
        BatchWriter batchWriter = new BatchWriter(
                    new ThreadPoolBuilder().build(),
                    writer,
                    timeout,
                    bufferedMetrics,
                    context);

        ListenableFuture<List<Boolean>> futures = batchWriter.apply(testdata);
        List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());

        //Confirm that each batch finished
        for (Boolean batchStatus : persisteds) {
            Assert.assertTrue(batchStatus);
        }
        //Confirm that each batch was inserted
        for (List<IMetric> l : testdata) {
          verify(writer).insertFullMetrics((List<Metric>)(List<?>)l);
          for (IMetric m : l) {
              verify(context).update(m.getCollectionTime(), Util.getShard(m.getLocator().toString()));
          }
        }
    }
}
