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
import com.rackspacecloud.blueflood.types.Locator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BatchedMetricsJSONOutputSerializerTest {
    private final Set<BasicRollupsOutputSerializer.MetricStat> filterStats;
    private static final String tenantId = "879890";

    public BatchedMetricsJSONOutputSerializerTest() {
        filterStats = new HashSet<BasicRollupsOutputSerializer.MetricStat>();
        filterStats.add(BasicRollupsOutputSerializer.MetricStat.AVERAGE);
        filterStats.add(BasicRollupsOutputSerializer.MetricStat.MIN);
        filterStats.add(BasicRollupsOutputSerializer.MetricStat.MAX);
        filterStats.add(BasicRollupsOutputSerializer.MetricStat.NUM_POINTS);
    }

    @Test
    public void testBatchedMetricsSerialization() throws Exception {
        final BatchedMetricsJSONOutputSerializer serializer = new BatchedMetricsJSONOutputSerializer();

        final Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();
        for (int i = 0; i < 2; i++) {
            final MetricData metricData = new MetricData(FakeMetricDataGenerator.generateFakeRollupPoints(), "unknown",
                    MetricData.Type.NUMBER);

            metrics.put(Locator.createLocatorFromPathComponents(tenantId, String.valueOf(i)), metricData);
        }

        JSONObject jsonMetrics = serializer.transformRollupData(metrics, filterStats);

        Assert.assertTrue(jsonMetrics.get("metrics") != null);
        JSONArray jsonMetricsArray = (JSONArray) jsonMetrics.get("metrics");

        Iterator<JSONObject> metricsObjects = jsonMetricsArray.iterator();
        Assert.assertTrue(metricsObjects.hasNext());

        while (metricsObjects.hasNext()) {
            JSONObject singleMetricObject = metricsObjects.next();
            Assert.assertTrue(singleMetricObject.get("unit").equals("unknown"));
            Assert.assertTrue(singleMetricObject.get("type").equals("number"));
            JSONArray data = (JSONArray) singleMetricObject.get("data");
            Assert.assertTrue(data != null);
        }
    }
}
