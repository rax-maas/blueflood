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
import java.util.concurrent.TimeUnit;

public class IMetricSerializerTest {
    IMetricSerializer ser = new IMetricSerializer();
    static final Locator goneIn = Locator.createLocatorFromPathComponents("gone", "in");
    static final TimeValue sixtySeconds = new TimeValue(60l, TimeUnit.SECONDS);
    static final ObjectMapper map = new IMetricSerializer().getObjectMapper();

    @Test
    public void testCounterSerialization() throws IOException {
        String counterValue = "{\"type\":\"counter\",\"count\":1,\"rate\":0.06666666666666667,\"sampleCount\":0}";

        CounterRollup counterDeserialized = ser.getObjectMapper().readValue(counterValue, CounterRollup.class);
        String counterSerialized = ser.getObjectMapper().writeValueAsString(counterDeserialized);
        Assert.assertEquals(counterValue, counterSerialized);

        CounterRollup counterReserialized = ser.getObjectMapper().readValue(counterSerialized, CounterRollup.class);
        Assert.assertEquals(counterDeserialized, counterReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, counterReserialized);
        String preagg = map.writeValueAsString(m);
        Assert.assertTrue(preagg.startsWith("{\"class\":\"preaggregated\",\"collectionTime\":12345,\"tenantId\":\"gone\",\"metricName\":\"in\",\"metricValue\":{\"type\":\""));
        Assert.assertTrue(preagg.endsWith("},\"ttlInSeconds\":60}"));
    }

    @Test
    public void testGaugeSerialization() throws IOException {
        String gaugeValue = "{\"type\":\"gauge\",\"count\":1,\"timestamp\":1389211230,\"latestNumericValue\":397,\"mean\":397,\"var\":0.0,\"min\":397,\"max\":397}";

        GaugeRollup gaugeDeserialized = ser.getObjectMapper().readValue(gaugeValue, GaugeRollup.class);
        String gaugeSerialized = ser.getObjectMapper().writeValueAsString(gaugeDeserialized);
        Assert.assertEquals(gaugeValue, gaugeSerialized);

        GaugeRollup gaugeReserialized = ser.getObjectMapper().readValue(gaugeSerialized, GaugeRollup.class);
        Assert.assertEquals(gaugeDeserialized, gaugeReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, gaugeReserialized);
        String preagg = map.writeValueAsString(m);
        Assert.assertTrue(preagg.startsWith("{\"class\":\"preaggregated\",\"collectionTime\":12345,\"tenantId\":\"gone\",\"metricName\":\"in\",\"metricValue\":{\"type\":\""));
        Assert.assertTrue(preagg.endsWith("},\"ttlInSeconds\":60}"));
    }

    @Test
    public void testSetSerialization() throws IOException {
        String setValue = "{\"type\":\"set\",\"hashes\":[746007989,1875251108,98262,103159993,1727114331,-1034140067,98699,1062516268,99644],\"count\":9}";

        SetRollup setDeserialized = ser.getObjectMapper().readValue(setValue, SetRollup.class);
        String setSerialized = ser.getObjectMapper().writeValueAsString(setDeserialized);
        Assert.assertEquals(setValue, setSerialized);

        SetRollup setReserialized = ser.getObjectMapper().readValue(setSerialized, SetRollup.class);
        Assert.assertEquals(setDeserialized, setReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, setReserialized);
        String preagg = map.writeValueAsString(m);
        Assert.assertTrue(preagg.startsWith("{\"class\":\"preaggregated\",\"collectionTime\":12345,\"tenantId\":\"gone\",\"metricName\":\"in\",\"metricValue\":{\"type\":\""));
        Assert.assertTrue(preagg.endsWith("},\"ttlInSeconds\":60}"));
    }

    @Test
    public void testTimerSerialization() throws IOException {
        String timerValue = "{\"type\":\"timer\",\"sum\":214,\"count\":1,\"rate\":0.06666666666666667,\"sampleCount\":1,\"min\":214,\"max\":214,\"average\":214,\"variance\":0.0,\"percentiles\":{\"98\":214,\"99\":214,\"75\":214,\"999\":214,\"50\":214}}";

        TimerRollup timerDeserialized = ser.getObjectMapper().readValue(timerValue, TimerRollup.class);
        String timerSerialized = ser.getObjectMapper().writeValueAsString(timerDeserialized);
        Assert.assertEquals(timerValue, timerSerialized);

        TimerRollup timerReserialized = ser.getObjectMapper().readValue(timerSerialized, TimerRollup.class);
        Assert.assertEquals(timerDeserialized, timerReserialized);

        PreaggregatedMetric m = new PreaggregatedMetric(12345l, goneIn, sixtySeconds, timerReserialized);
        String preagg = map.writeValueAsString(m);
        Assert.assertTrue(preagg.startsWith("{\"class\":\"preaggregated\",\"collectionTime\":12345,\"tenantId\":\"gone\",\"metricName\":\"in\",\"metricValue\":{\"type\":\""));
        Assert.assertTrue(preagg.endsWith("},\"ttlInSeconds\":60}"));
    }
}
