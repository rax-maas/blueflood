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

package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public class IMetricSerializerTest {
    static final Locator goneIn = Locator.createLocatorFromPathComponents("gone", "in");
    static final TimeValue sixtySeconds = new TimeValue(60l, TimeUnit.SECONDS);
    static final ObjectMapper mapper = new IMetricSerializer().getObjectMapper();
    private static final String START_STRING = "{\"class\":\"preaggregated\",\"collectionTime\":12345,\"tenantId\":\"gone\",\"metricName\":\"in\",\"metricValue\":{\"type\":\"";
    private static final String END_STRING = "},\"ttlInSeconds\":60}";

    @Test
    public void testCounterSerialization() throws IOException {
        String counterValue = "{\"type\":\"counter\",\"count\":1,\"rate\":0.06666666666666667,\"sampleCount\":0}";

        BluefloodCounterRollup counterDeserialized = mapper.readValue(counterValue, BluefloodCounterRollup.class);
        String counterSerialized = mapper.writeValueAsString(counterDeserialized);
        Assert.assertEquals(counterValue, counterSerialized);

        BluefloodCounterRollup counterReserialized = mapper.readValue(counterSerialized, BluefloodCounterRollup.class);
        Assert.assertEquals(counterDeserialized, counterReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, counterReserialized);
        String preagg = mapper.writeValueAsString(m);
        Assert.assertTrue(preagg, preagg.startsWith(START_STRING));
        Assert.assertTrue(preagg, preagg.endsWith(END_STRING));
    }

    @Test
    public void testGaugeSerialization() throws IOException {
        String gaugeValue = "{\"type\":\"gauge\",\"count\":1,\"latestNumericValue\":397,\"max\":397,\"mean\":397,\"min\":397,\"timestamp\":1389211230,\"var\":0.0}";

        BluefloodGaugeRollup gaugeDeserialized = mapper.readValue(gaugeValue, BluefloodGaugeRollup.class);
        String gaugeSerialized = mapper.writeValueAsString(gaugeDeserialized);
        Assert.assertEquals(gaugeValue, gaugeSerialized);

        BluefloodGaugeRollup gaugeReserialized = mapper.readValue(gaugeSerialized, BluefloodGaugeRollup.class);
        Assert.assertEquals(gaugeDeserialized, gaugeReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, gaugeReserialized);
        String preagg = mapper.writeValueAsString(m);
        Assert.assertTrue(preagg, preagg.startsWith(START_STRING));
        Assert.assertTrue(preagg, preagg.endsWith(END_STRING));
    }

    @Test
    public void testSetSerialization() throws IOException {
        String setValue = "{\"type\":\"set\",\"count\":9,\"hashes\":[746007989,1875251108,98262,103159993,1727114331,-1034140067,98699,1062516268,99644]}";

        BluefloodSetRollup setDeserialized = mapper.readValue(setValue, BluefloodSetRollup.class);
        String setSerialized = mapper.writeValueAsString(setDeserialized);
        Assert.assertEquals(setValue, setSerialized);

        BluefloodSetRollup setReserialized = mapper.readValue(setSerialized, BluefloodSetRollup.class);
        Assert.assertEquals(setDeserialized, setReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, setReserialized);
        String preagg = mapper.writeValueAsString(m);
        Assert.assertTrue(preagg, preagg.startsWith(START_STRING));
        Assert.assertTrue(preagg, preagg.endsWith(END_STRING));
    }

    @Test
    public void testTimerSerialization() throws IOException {
        String timerValue = "{\"type\":\"timer\",\"average\":214,\"count\":1,\"max\":214,\"min\":214,\"percentiles\":{\"98\":214,\"99\":214,\"75\":214,\"999\":214,\"50\":214},\"rate\":0.06666666666666667,\"sampleCount\":1,\"sum\":214.0,\"variance\":0.0}";

        BluefloodTimerRollup timerDeserialized = mapper.readValue(timerValue, BluefloodTimerRollup.class);
        String timerSerialized = mapper.writeValueAsString(timerDeserialized);
        Assert.assertEquals(timerValue, timerSerialized);

        BluefloodTimerRollup timerReserialized = mapper.readValue(timerSerialized, BluefloodTimerRollup.class);
        Assert.assertEquals(timerDeserialized, timerReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, timerReserialized);
        String preagg = mapper.writeValueAsString(m);
        Assert.assertTrue(preagg, preagg.startsWith(START_STRING));
        Assert.assertTrue(preagg, preagg.endsWith(END_STRING));
    }

    @Test
    public void testRawSerialization() throws IOException {
        Metric metric = new Metric(goneIn, 5, 12345l, sixtySeconds, "bytes");
        String serialized = mapper.writeValueAsString(metric);

        Metric deserialized = mapper.readValue(serialized, Metric.class);
        String reserialized = mapper.writeValueAsString(deserialized);
        Assert.assertEquals(metric, deserialized);
        Assert.assertEquals(serialized, reserialized);
    }

    @Test
    public void testBigIntMetricConvertsIntoDouble() {
        Metric metric = new Metric(goneIn, BigInteger.valueOf(12345l), 122345l,sixtySeconds,"bigguys");
        Assert.assertTrue(metric.getMetricValue() instanceof Double);
    }
}