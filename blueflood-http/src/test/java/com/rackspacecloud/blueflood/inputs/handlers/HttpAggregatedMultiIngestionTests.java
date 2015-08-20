/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.inputs.handlers;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.AggregatedPayload;
import com.rackspacecloud.blueflood.io.serializers.NumericSerializer;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class HttpAggregatedMultiIngestionTests {
    
    private List<AggregatedPayload> bundleList;
    
    @Before
    public void buildBundle() throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_multi_bundle.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();
        bundleList = HttpAggregatedMultiIngestionHandler.createBundleList(json);
    }

    @Test
    public void testMultiBundle() {
        HashSet<String> tenantIdSet = new HashSet<String>();
        HashSet<Long> timestampSet = new HashSet<Long>();
        Assert.assertTrue(bundleList.size() == 3);

        for (AggregatedPayload bundle : bundleList) {
            tenantIdSet.add(bundle.getTenantId());
            timestampSet.add(bundle.getTimestamp());
        }

        Assert.assertTrue(tenantIdSet.size() == 3); //3 unique timestamps are supported
        Assert.assertTrue(timestampSet.size() == 3); //3 unique tenants are supported
    }

    @Test
    public void testCounters() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> counters = PreaggregateConversions.convertCounters("1", 1, 15000, bundle.getCounters());
            Assert.assertEquals(1, counters.size());
            ensureSerializability(counters);
        }
    }

    @Test
    public void testEmptyButValidMultiJSON() {
        String badJson = "[]";
        List<AggregatedPayload> bundle = HttpAggregatedMultiIngestionHandler.createBundleList(badJson);
    }
    
    @Test
    public void testGauges() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> gauges = PreaggregateConversions.convertGauges("1", 1, bundle.getGauges());
            Assert.assertEquals(1, gauges.size());
            ensureSerializability(gauges);
        }
    }
     
    @Test
    public void testSets() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> sets = PreaggregateConversions.convertSets("1", 1, bundle.getSets());
            Assert.assertEquals(1, sets.size());
            ensureSerializability(sets);
        }
    }
    
    @Test
    public void testTimers() {
        for (AggregatedPayload bundle : bundleList) {
            Collection<PreaggregatedMetric> timers = PreaggregateConversions.convertTimers("1", 1, bundle.getTimers());
            Assert.assertEquals(1, timers.size());
            ensureSerializability(timers);
        }
    }
    
    // ok. while we're out it, let's test serialization. Just for fun. The reasoning is that these metrics
    // follow a different creation path that what we currently have in tests.
    private static void ensureSerializability(Collection<PreaggregatedMetric> metrics) {
        for (PreaggregatedMetric metric : metrics) {
            AbstractSerializer serializer = NumericSerializer.serializerFor(metric.getMetricValue().getClass());
            serializer.toByteBuffer(metric.getMetricValue());
        }
    }
}
