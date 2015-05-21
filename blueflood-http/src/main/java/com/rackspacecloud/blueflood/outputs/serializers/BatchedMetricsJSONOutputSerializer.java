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
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;

public class BatchedMetricsJSONOutputSerializer extends JSONBasicRollupsOutputSerializer
        implements BatchedMetricsOutputSerializer<JSONObject> {

    @Override
    public JSONObject transformRollupData(Map<Locator, MetricData> metricData, Set<MetricStat> filterStats)
            throws SerializationException {
        final JSONObject globalJSON = new JSONObject();
        final JSONArray metricsArray = new JSONArray();

        for (Map.Entry<Locator, MetricData> one : metricData.entrySet()) {
            final JSONObject singleMetricJSON = new JSONObject();
            singleMetricJSON.put("metric", one.getKey().getMetricName());
            singleMetricJSON.put("unit", one.getValue().getUnit() == null ? Util.UNKNOWN : one.getValue().getUnit());
            singleMetricJSON.put("type", one.getValue().getType());
            JSONArray values = transformDataToJSONArray(one.getValue(), filterStats);
            singleMetricJSON.put("data", values);
            metricsArray.add(singleMetricJSON);
        }

        globalJSON.put("metrics", metricsArray);
        return globalJSON;
    }
}

