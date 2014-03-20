/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.serializers.IMetricKafkaSerializer;
import com.rackspacecloud.blueflood.io.serializers.IMetricSerializer;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTest {

    @Test
    public void testKafkaSerializerFullRes() throws Exception {
        IMetricSerializer ser = new IMetricKafkaSerializer(new VerifiableProperties());

        MetricsCollection before = makeCollection();
//        MetricsCollection after = ser.fromBytes(ser.toBytes(before));

//        Collection<IMetric> metricsBefore = before.toMetrics();
//        Collection<IMetric> metricsAfter = after.toMetrics();

        Metric metricBefore = makeMetric();
        byte[] bytes = ser.getObjectMapper().writeValueAsBytes(metricBefore);

        System.out.println("Serialized value vvvb");
        String serVal = ser.getObjectMapper().writeValueAsString(metricBefore);
        System.out.println(serVal);
//        System.out.println((bytes.toString()));

        Metric metricAfter = ser.getObjectMapper().readValue(serVal, Metric.class);
        Assert.assertEquals(metricBefore, metricAfter);
//        Assert.assertEquals(metricsBefore, metricsAfter);
//        Assert.assertEquals(before, after);
    }

    @Test
    public void testKafkaSerializerPreaggregated() throws Exception {
        IMetricSerializer ser = new IMetricKafkaSerializer(new VerifiableProperties());
    }



    @Test
    public void testKafkaProducer() throws Exception{
//        KafkaProducer producer = KafkaProducer.getInstance();
//        producer.pushFullResBatch(makeCollection());
    }

    @Test
    public void testKafkaConsumer() throws InterruptedException {
//        KafkaConsumer consumer = new KafkaConsumer();
//        Thread.sleep(10000l);
    }

    private MetricsCollection makeCollection() {
        MetricsCollection mb = new MetricsCollection();
        List<IMetric> metrics = new ArrayList<IMetric>();
        metrics.add(makeMetric());
        mb.add(metrics);
        return mb;
    }

    private Metric makeMetric() {
        Locator loc = Locator.createLocatorFromDbKey("foo.bar.baz");
        Metric m = new Metric(loc, 12345, System.currentTimeMillis(), new TimeValue(30l, TimeUnit.DAYS), "bytes");
        return m;
    }
}