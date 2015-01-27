/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
    private static final int NUM_LISTS = 4;
    private static final int METRICS_PER_LIST = 4;

    static <T extends IMetric> List<List<IMetric>> createTestData(Class<T> c) {
        // create fake metrics to test
        Integer counter = 0;
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        for (int i = 0; i < NUM_LISTS; i++) {
            List<IMetric> l = new ArrayList<IMetric>();
            testdata.add(l);
            for (int j = 0; j < METRICS_PER_LIST; j++) {
                counter++;
                IMetric m = mock(c);
                l.add(m);
                // setup the metric stubs
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
            new ThreadPoolBuilder().build(), writer, timeout, bufferedMetrics,
                context);
        
        ListenableFuture<List<Boolean>> futures = batchWriter.apply(allTestdata);
        List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());

        //Confirm correct number of futures, (the 2 is because we have both
        //  simple and PreaggregatedMetric's)
        Assert.assertTrue(persisteds.size() == (NUM_LISTS * 2));

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
            Assert.assertTrue(l.size() == METRICS_PER_LIST);
            for (IMetric m : l) {
                verify(context).update(m.getCollectionTime(), 
                    Util.getShard(m.getLocator().toString()));
            }
        }
    }
}
