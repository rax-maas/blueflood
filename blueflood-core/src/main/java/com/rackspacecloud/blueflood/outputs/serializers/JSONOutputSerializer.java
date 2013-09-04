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
import com.rackspacecloud.blueflood.types.Rollup;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;

public class JSONOutputSerializer implements OutputSerializer<JSONObject> {

    @Override
    public JSONObject transformRollupData(MetricData metricData, Set<String> filterStats)
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

    private JSONArray transformDataToJSONArray(MetricData metricData, Set<String> filterStats) {
        Points points = metricData.getData();
        final JSONArray data = new JSONArray();
        final Set<Map.Entry<Long, Points.Point>> dataPoints = points.getPoints().entrySet();
        for (Map.Entry<Long, Points.Point> point : dataPoints) {
            data.add(toJSON(point.getKey(), point.getValue(), metricData.getUnit(), filterStats));
        }

        return data;
    }

    private JSONObject toJSON(long timestamp, Points.Point point, String unit, Set<String> filterStats) {
        final JSONObject  object = new JSONObject();
        object.put("timestamp", timestamp);
        object.put("unit", unit);

        JSONObject filterStatsObject;
        long numPoints;
        if (point.getData() instanceof Rollup) {
            numPoints = ((Rollup) point.getData()).getCount();
            filterStatsObject = getFilteredStatsForRollup((Rollup) point.getData(), filterStats);
        } else {
            numPoints = 1;
            filterStatsObject = getFilteredStatsForFullRes(point.getData(), filterStats);
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

    private JSONObject getFilteredStatsForRollup(Rollup rollup, Set<String> filterStats) {
        final JSONObject filteredObject = new JSONObject();
            for (String stat : filterStats) {
            String lowerCaseStat = stat.toLowerCase();
            if (lowerCaseStat.equals("average")) {
                filteredObject.put("average", rollup.getAverage());
            } else if (lowerCaseStat.equals("min")) {
                filteredObject.put("min", rollup.getMinValue());
            } else if (lowerCaseStat.equals("max")) {
                filteredObject.put("max", rollup.getMaxValue());
            } else if (lowerCaseStat.equals("variance")) {
                filteredObject.put("variance", rollup.getVariance());
            } else if (lowerCaseStat.equals("numpoints")) {
                filteredObject.put("numPoints", rollup.getCount());
            }
        }

        return filteredObject;
    }

    private JSONObject getFilteredStatsForFullRes(Object rawSample, Set<String> filterStats) {
        final JSONObject filteredObject = new JSONObject();
        if (rawSample instanceof String || rawSample instanceof Boolean) {
            filteredObject.put("value", rawSample);
        } else {
            for (String stat : filterStats) {
                String lowerCaseStat = stat.toLowerCase();
                if (lowerCaseStat.equals("average")) {
                    filteredObject.put("average", rawSample);
                } else if (lowerCaseStat.equals("min")) {
                    filteredObject.put("min", rawSample);
                } else if (lowerCaseStat.equals("max")) {
                    filteredObject.put("max", rawSample);
                } else if (lowerCaseStat.equals("variance")) {
                    filteredObject.put("variance", 0);
                } else if (lowerCaseStat.equals("numpoints")) {
                    filteredObject.put("numPoints", 1);
                }
            }
        }
        return filteredObject;
    }
}
