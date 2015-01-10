package com.rackspacecloud.blueflood.inputs.processors;
import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.IngestionContext;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
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

    static <T extends IMetric> List<List<IMetric>> createTestData(Class<T> c) {
        List<IMetric> l1 = new ArrayList<IMetric>();
        l1.add(mock(c));
        l1.add(mock(c));
        l1.add(mock(c));
        l1.add(mock(c));

        List<IMetric> l2 = new ArrayList<IMetric>();
        l2.add(mock(c));
        l2.add(mock(c));
        l2.add(mock(c));
        l2.add(mock(c));

        List<IMetric> l3 = new ArrayList<IMetric>();
        l3.add(mock(c));
        l3.add(mock(c));
        l3.add(mock(c));
        l3.add(mock(c));

        List<IMetric> l4 = new ArrayList<IMetric>();
        l4.add(mock(c));
        l4.add(mock(c));
        l4.add(mock(c));
        l4.add(mock(c));

        // create fake metrics to test
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        testdata.add(l1);
        testdata.add(l2);
        testdata.add(l3);
        testdata.add(l4);

        Integer counter = 0;
        // setup the stubs
        for (List<IMetric> l : testdata) {
            for (IMetric m : l) {
                counter++;
                stub(m.getLocator()).
                    toReturn(Locator.createLocatorFromDbKey(c.toString() + counter.toString()));
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
        List<List<IMetric>> testdata = createTestData(Metric.class);
        List<List<IMetric>> pTestdata = createTestData(PreaggregatedMetric.class);
        List<List<IMetric>> allTestdata = new ArrayList<List<IMetric>>();
        allTestdata.addAll(testdata);
        allTestdata.addAll(pTestdata);

        BatchWriter batchWriter = new BatchWriter(
                    new ThreadPoolBuilder().build(),
                    writer,
                    timeout,
                    bufferedMetrics,
                    context);
        
        ListenableFuture<List<Boolean>> futures = batchWriter.apply(allTestdata);
        List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());

        //Confirm that each batch finished
        for (Boolean batchStatus : persisteds) {
            Assert.assertTrue(batchStatus);
        }

        //Confirm that each regular batch was inserted
        for (List<IMetric> l : testdata) {
            verify(writer).insertFullMetrics((List<Metric>)(List<?>)l);
        }

        //Confirm that each preagg batch was inserted
        for (List<IMetric> l : pTestdata) {
            verify(writer).insertPreaggreatedMetrics(l);
        }

        //Confirm scheduleContext was updated
        for (List<IMetric> l : allTestdata) {
            for (IMetric m : l) {
                verify(context).update(m.getCollectionTime(), 
                    Util.getShard(m.getLocator().toString()));
            }
        }
    }
}
