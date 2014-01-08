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

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.Points;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class JSONHistogramOutputSerializerTest {

    @Test
    public void testHistogramRollupsSerialization() throws SerializationException {
        final JSONHistogramOutputSerializer serializer = new JSONHistogramOutputSerializer();
        final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeHistogramRollupPoints(), "unknown",
                MetricData.Type.HISTOGRAM);

        JSONObject metricDataJSON = serializer.transformHistogram(metricData);

        final JSONArray data = (JSONArray) metricDataJSON.get("values");

        for (int i = 0; i < data.size(); i++ ) {
            final JSONObject dataJSON = (JSONObject) data.get(i);
            final Points.Point point = (Points.Point) metricData.getData().getPoints().get(dataJSON.get("timestamp"));

            JSONArray hist = (JSONArray) dataJSON.get("histogram");
            Assert.assertNotNull(hist);

            for (int j = 0; j < hist.size(); j++) {
                JSONObject bin = (JSONObject) hist.get(j);
                Assert.assertNotNull(bin.get("count"));
                Assert.assertNotNull(bin.get("mean"));
            }
        }
    }
}
