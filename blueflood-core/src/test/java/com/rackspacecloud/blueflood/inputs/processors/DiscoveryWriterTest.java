/*
 * Copyright 2013-2015 Rackspace
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

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.stub;


public class DiscoveryWriterTest {
    static List<List<IMetric>> createTestData() {
        List<IMetric> l1 = new ArrayList<IMetric>();
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));
        l1.add(mock(IMetric.class));

        List<IMetric> l2 = new ArrayList<IMetric>();
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));
        l2.add(mock(IMetric.class));

        List<IMetric> l3 = new ArrayList<IMetric>();
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));
        l3.add(mock(IMetric.class));

        List<IMetric> l4 = new ArrayList<IMetric>();
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));
        l4.add(mock(IMetric.class));


        // create fake metrics to test
        List<List<IMetric>> testdata = new ArrayList<List<IMetric>>();
        testdata.add(l1);
        testdata.add(l2);
        testdata.add(l3);
        testdata.add(l4);

        //setup locators
        Integer counter = 0;
        for (List<IMetric> l : testdata) {
            for (IMetric m : l) {
                counter++;
                stub(m.getLocator()).toReturn(Locator.createLocatorFromDbKey(counter.toString()));
            }
        }
        
        return testdata;
    }

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

        List<List<IMetric>> testdata = createTestData();

        List<IMetric> flatTestData = new ArrayList<IMetric>();
        for (List<IMetric> list : testdata) {
            if (list.size() == 0) continue;
            for (IMetric m : list) {
                flatTestData.add(m);
            }
        }

        // Make sure we have data
        Assert.assertTrue(flatTestData.size() > 0);

        ListenableFuture<Boolean> result = discWriter.processMetrics(testdata);
        // wait until DiscoveryWriter finishes processing all the fake metrics
        Assert.assertTrue(result.get());

        // verify the insertDiscovery method on all implementors of
        // DiscoveryIO has been called on all metrics
        verify(discovererA).insertDiscovery(flatTestData);
        verify(discovererB).insertDiscovery(flatTestData);
    }
}
