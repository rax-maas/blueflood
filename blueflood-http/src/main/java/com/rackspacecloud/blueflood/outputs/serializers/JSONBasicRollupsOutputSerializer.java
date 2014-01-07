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
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;

public class JSONBasicRollupsOutputSerializer implements BasicRollupsOutputSerializer<JSONObject> {

    @Override
    public JSONObject transformRollupData(MetricData metricData, Set<MetricStat> filterStats)
            throws SerializationException {
        final JSONObject globalJSON = new JSONObject();
        final JSONObject metaObject = new JSONObject();

        final JSONArray valuesArray = transformDataToJSONArray(metricData, filterStats);

        metaObject.put("count", valuesArray.size());
        metaObject.put("limit", null);
        metaObject.put("marker", null);
        metaObject.put("next_href", null);
        globalJSON.put("values", valuesArray);
        globalJSON.put("metadata", metaObject);

        return globalJSON;
    }

    protected JSONArray transformDataToJSONArray(MetricData metricData, Set<MetricStat> filterStats)
            throws SerializationException {
        Points points = metricData.getData();
        final JSONArray data = new JSONArray();
        final Set<Map.Entry<Long, Points.Point>> dataPoints = points.getPoints().entrySet();
        for (Map.Entry<Long, Points.Point> point : dataPoints) {
            data.add(toJSON(point.getKey(), point.getValue(), metricData.getUnit(), filterStats));
        }

        return data;
    }

    private JSONObject toJSON(long timestamp, Points.Point point, String unit, Set<MetricStat> filterStats)
            throws SerializationException {
        final JSONObject  object = new JSONObject();
        object.put("timestamp", timestamp);

        JSONObject filterStatsObject = null;
        long numPoints = 1;
        if (point.getData() instanceof BasicRollup) {
            numPoints = ((BasicRollup) point.getData()).getCount();
            filterStatsObject = getFilteredStatsForRollup((BasicRollup) point.getData(), filterStats);
        } else if (point.getData() instanceof SimpleNumber) {
            numPoints = 1;
            filterStatsObject = getFilteredStatsForFullRes(point.getData(), filterStats);
        } else if (point.getData() instanceof String) {
            numPoints = 1;
            filterStatsObject = getFilteredStatsForString((String) point.getData());
        } else if (point.getData() instanceof Boolean) {
            numPoints = 1;
            filterStatsObject = getFilteredStatsForBoolean((Boolean) point.getData());
        } else {
            throw new SerializationException("Unsupported data type for Point");
        }

        // Set all filtered stats to null if numPoints is 0
        if (numPoints == 0) {
            final Set<Map.Entry<String, Object>> statsSet = filterStatsObject.entrySet();

            for (Map.Entry<String, Object> stat : statsSet) {
                if (!stat.getKey().equals("numPoints")) {
                    stat.setValue(null);
                }
            }
        }

        // Add filtered stats to main object
        final Set<Map.Entry<String, Object>> statsSet = filterStatsObject.entrySet();
        for (Map.Entry<String, Object> stat : statsSet) {
            object.put(stat.getKey(), stat.getValue());
        }

        return object;
    }

    private JSONObject getFilteredStatsForRollup(BasicRollup rollup, Set<MetricStat> filterStats) {
        final JSONObject filteredObject = new JSONObject();
        for (MetricStat stat : filterStats) {
            filteredObject.put(stat.toString(), stat.convertBasicRollupToObject(rollup));
        }
        return filteredObject;
    }

    private JSONObject getFilteredStatsForFullRes(Object rawSample, Set<MetricStat> filterStats) {
        final JSONObject filteredObject = new JSONObject();
        if (rawSample instanceof String || rawSample instanceof Boolean) {
            filteredObject.put("value", rawSample);
        } else {
            for (MetricStat stat : filterStats) {
                filteredObject.put(stat.toString(), stat.convertRawSampleToObject(((SimpleNumber) rawSample).getValue()));
            }
        }
        return filteredObject;
    }

    private JSONObject getFilteredStatsForString(String value) {
        final JSONObject filteredObject = new JSONObject();
        filteredObject.put("value", value);

        return filteredObject;
    }

    private JSONObject getFilteredStatsForBoolean(Boolean value) {
        final JSONObject filteredObject = new JSONObject();
        filteredObject.put("value", value);

        return filteredObject;
    }
}
