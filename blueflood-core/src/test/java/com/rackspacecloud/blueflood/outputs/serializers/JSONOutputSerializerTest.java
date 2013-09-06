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

import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.OutputSerializer.MetricStat;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class JSONOutputSerializerTest {
    private final Set<MetricStat> filterStats;

    public JSONOutputSerializerTest() {
        filterStats = new HashSet<MetricStat>();
        filterStats.add(MetricStat.AVERAGE);
        filterStats.add(MetricStat.MIN);
        filterStats.add(MetricStat.MAX);
    }

    @Test
    public void testTransformRollupDataAtFullRes() throws Exception {
        final JSONOutputSerializer serializer = new JSONOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeFullResPoints(), "unknown");

        JSONObject metricDataJSON = serializer.transformRollupData(metricData, filterStats);

        final JSONArray data = (JSONArray) metricDataJSON.get("values");
        for (int i = 0; i < data.size(); i++) {
            final JSONObject dataJSON = (JSONObject) data.get(i);
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(dataJSON.get("timestamp"));

            Assert.assertEquals(point.getData(), dataJSON.get("average"));
            Assert.assertEquals(point.getData(), dataJSON.get("min"));
            Assert.assertEquals(point.getData(), dataJSON.get("max"));

            // Assert unit is same
            Assert.assertEquals(metricData.getUnit(), dataJSON.get("unit"));

            // Assert that variance isn't present
            Assert.assertNull(dataJSON.get("variance"));

            // Assert numPoints isn't present
            Assert.assertNull(dataJSON.get("numPoints"));
        }
    }


    @Test
    public void testTransformRollupDataForCoarserGran() throws Exception {
        final JSONOutputSerializer serializer = new JSONOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeRollupPoints(), "unknown");
        Set<MetricStat> filters = new HashSet<MetricStat>();
        filters.add(MetricStat.AVERAGE);
        filters.add(MetricStat.MIN);
        filters.add(MetricStat.MAX);
        filters.add(MetricStat.NUM_POINTS);

        JSONObject metricDataJSON = serializer.transformRollupData(metricData, filters);

        final JSONArray data = (JSONArray) metricDataJSON.get("values");
        for (int i = 0; i < data.size(); i++) {
            final JSONObject dataJSON = (JSONObject) data.get(i);
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(dataJSON.get("timestamp"));

            long numPoints = ((Rollup) point.getData()).getCount();
            Assert.assertEquals(numPoints, dataJSON.get("numPoints"));

            if (numPoints == 0) {
                Assert.assertNull(dataJSON.get("average"));
                Assert.assertNull(dataJSON.get("min"));
                Assert.assertNull(dataJSON.get("max"));
            } else {
                Assert.assertEquals(((Rollup) point.getData()).getAverage(), dataJSON.get("average"));
                Assert.assertEquals(((Rollup) point.getData()).getMaxValue(), dataJSON.get("max"));
                Assert.assertEquals(((Rollup) point.getData()).getMinValue(), dataJSON.get("min"));
            }

            // Assert unit is same
            Assert.assertEquals(metricData.getUnit(), dataJSON.get("unit"));

            // Assert that variance isn't present
            Assert.assertNull(dataJSON.get("variance"));
        }
    }

    @Test
    public void testTransformRollupDataString() throws SerializationException{
        final JSONOutputSerializer serializer = new JSONOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeStringPoints(), "unknown");

        JSONObject metricDataJSON = serializer.transformRollupData(metricData, filterStats);
        final JSONArray data = (JSONArray) metricDataJSON.get("values");
        for (int i = 0; i < data.size(); i++ ) {
            final JSONObject dataJSON = (JSONObject) data.get(i);
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(dataJSON.get("timestamp"));

            Assert.assertEquals(point.getData(), dataJSON.get("value"));
            Assert.assertEquals(1L, dataJSON.get("numPoints"));

            Assert.assertEquals(metricData.getUnit(), dataJSON.get("unit"));

            Assert.assertNull(dataJSON.get("average"));
            Assert.assertNull(dataJSON.get("min"));
            Assert.assertNull(dataJSON.get("max"));
            Assert.assertNull(dataJSON.get("variance"));
        }
    }
}

