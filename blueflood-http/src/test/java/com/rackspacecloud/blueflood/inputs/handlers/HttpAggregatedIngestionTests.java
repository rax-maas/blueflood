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

import com.google.gson.internal.LazilyParsedNumber;
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

public class HttpAggregatedIngestionTests {
    
    private AggregatedPayload payload;
    
    @Before
    public void buildPayload() throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/sample_payload.json")));
        String curLine = reader.readLine();
        while (curLine != null) {
            sb = sb.append(curLine);
            curLine = reader.readLine();
        }
        String json = sb.toString();
        payload = HttpAggregatedIngestionHandler.createPayload(json);
    }
    
    @Test(expected = NumberFormatException.class)
    public void testExpectedGsonConversionFailure() {
        new LazilyParsedNumber("2.321").longValue();
    }
    
    @Test
    public void testGsonNumberConversions() {
        Number doubleNum = new LazilyParsedNumber("2.321");
        Assert.assertEquals(Double.parseDouble("2.321"), PreaggregateConversions.resolveNumber(doubleNum));
        
        Number longNum = new LazilyParsedNumber("12345");
        Assert.assertEquals(Long.parseLong("12345"), PreaggregateConversions.resolveNumber(longNum));
    }
    
    @Test
    public void testCounters() {
        Collection<PreaggregatedMetric> counters = PreaggregateConversions.convertCounters("1", 1, 15000, payload.getCounters());
        Assert.assertEquals(6, counters.size());
        ensureSerializability(counters);
    }
    
    @Test
    public void testGauges() {
        Collection<PreaggregatedMetric> gauges = PreaggregateConversions.convertGauges("1", 1, payload.getGauges());
        Assert.assertEquals(4, gauges.size());
        ensureSerializability(gauges);
    }
     
    @Test
    public void testSets() {
        Collection<PreaggregatedMetric> sets = PreaggregateConversions.convertSets("1", 1, payload.getSets());
        Assert.assertEquals(2, sets.size());
        ensureSerializability(sets);
    }
    
    @Test
    public void testTimers() {
        Collection<PreaggregatedMetric> timers = PreaggregateConversions.convertTimers("1", 1, payload.getTimers());
        Assert.assertEquals(4, timers.size());
        ensureSerializability(timers);
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
