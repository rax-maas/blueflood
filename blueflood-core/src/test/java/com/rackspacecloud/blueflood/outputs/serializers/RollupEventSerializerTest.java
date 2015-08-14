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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.outputs.serializers.helpers.RollupSerializationHelper;
import com.rackspacecloud.blueflood.types.*;
import junit.framework.Assert;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import java.io.IOError;
import java.io.IOException;

public class RollupEventSerializerTest {
    @Test
    public void testBasicRollupSerialization() {
        BasicRollup rollup = new BasicRollup();
        rollup.setCount(20);
        rollup.setAverage(10);
        rollup.setMax(20);
        rollup.setMin(5);
        rollup.setVariance(12);
        //Get the JSON object node from Rollup
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(rollup);
        Assert.assertEquals(resultNode.get("max").asLong(), rollup.getMaxValue().toLong());
        Assert.assertEquals(resultNode.get("min").asLong(), rollup.getMinValue().toLong());
        Assert.assertEquals(resultNode.get("mean").asLong(), rollup.getAverage().toLong());
        Assert.assertEquals(resultNode.get("var").asDouble(), rollup.getVariance().toDouble());
        Assert.assertEquals(resultNode.get("count").asLong(), rollup.getCount());
    }

    @Test
    public void testTimerRollupSerialization() {
        BluefloodTimerRollup rollup = new BluefloodTimerRollup();
        rollup.withCount(20);
        rollup.withAverage(10);
        rollup.withMaxValue(20);
        rollup.withMinValue(5);
        rollup.withVariance(12);
        rollup.withSum(Double.valueOf(10));
        rollup.withCountPS(30);
        //Get the JSON object node from Rollup
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(rollup);
        Assert.assertEquals(resultNode.get("max").asLong(), rollup.getMaxValue().toLong());
        Assert.assertEquals(resultNode.get("min").asLong(), rollup.getMinValue().toLong());
        Assert.assertEquals(resultNode.get("mean").asLong(), rollup.getAverage().toLong());
        Assert.assertEquals(resultNode.get("var").asDouble(), rollup.getVariance().toDouble());
        Assert.assertEquals(resultNode.get("count").asLong(), rollup.getCount());
        Assert.assertEquals(resultNode.get("sum").asDouble(), rollup.getSum());
        Assert.assertEquals(resultNode.get("rate").asDouble(), rollup.getRate());
    }

    @Test
    public void testHistgramRollupSerialization() throws IOException {
        Points<SimpleNumber> points = new Points<SimpleNumber>();
        long startTime = 12345678L;
        //Count = 3.0, Mean = 2.0
        points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(1.0)));
        points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(2.0)));
        points.add(new Points.Point<SimpleNumber>(startTime++, new SimpleNumber(3.0)));
        HistogramRollup histogramRollup = HistogramRollup.buildRollupFromRawSamples(points);
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(histogramRollup);
        ArrayNode node = (ArrayNode)resultNode.get("bins");
        Assert.assertEquals(node.get(0).get("count").asDouble(), 3.0);
        Assert.assertEquals(node.get(0).get("mean").asDouble(), 2.0);
    }

    @Test
    public void testSetRollupSerialization() {
        final BluefloodSetRollup rollup0 = new BluefloodSetRollup()
                .withObject(10)
                .withObject(20)
                .withObject(30);
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(rollup0);
        Assert.assertEquals(resultNode.get("count").asInt(), 3);
    }

    @Test
    public void testGaugeRollupSerialization() {
        final BluefloodGaugeRollup rollup = new BluefloodGaugeRollup()
                .withLatest(0, 1234);
        rollup.setMin(1);
        rollup.setMax(2);
        rollup.setCount(1);
        rollup.setVariance(23);
        rollup.setAverage(4);
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(rollup);
        Assert.assertEquals(resultNode.get("max").asLong(), rollup.getMaxValue().toLong());
        Assert.assertEquals(resultNode.get("min").asLong(), rollup.getMinValue().toLong());
        Assert.assertEquals(resultNode.get("mean").asLong(), rollup.getAverage().toLong());
        Assert.assertEquals(resultNode.get("var").asDouble(), rollup.getVariance().toDouble());
        Assert.assertEquals(resultNode.get("count").asLong(), rollup.getCount());
        Assert.assertEquals(resultNode.get("latestVal").asLong(), rollup.getLatestNumericValue().longValue());
    }

    //Passing an unknown rollup type will throw IOError
    @Test(expected = IOError.class)
    public void testExceptionOnInvalid() {
        class TestRollup implements Rollup{
            @Override
            public Boolean hasData() {
                return null;
            }

            @Override
            public RollupType getRollupType() {
                return null;
            }
        };
        RollupSerializationHelper.rollupToJson(new TestRollup());
    }

    @Test
    public void testNullValuesOnZeroCount() {
        BasicRollup rollup = new BasicRollup();
        rollup.setCount(0);
        //Get the JSON object node from Rollup
        ObjectNode resultNode = RollupSerializationHelper.rollupToJson(rollup);
        Assert.assertTrue(resultNode.get("max").isNull());
        Assert.assertTrue(resultNode.get("min").isNull());
        Assert.assertTrue(resultNode.get("mean").isNull());
        Assert.assertTrue(resultNode.get("var").isNull());
        Assert.assertEquals(resultNode.get("count").asLong(), 0);
    }
}
