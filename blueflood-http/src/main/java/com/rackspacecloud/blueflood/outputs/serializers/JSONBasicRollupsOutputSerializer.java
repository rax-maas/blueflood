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
import com.rackspacecloud.blueflood.outputs.utils.PlotRequestParser;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class JSONBasicRollupsOutputSerializer implements BasicRollupsOutputSerializer<JSONObject> {
    private static final Logger log = LoggerFactory.getLogger(JSONBasicRollupsOutputSerializer.class);
    
    @Override
    public JSONObject transformRollupData(MetricData metricData, Set<MetricStat> filterStats)
            throws SerializationException {
        final JSONObject globalJSON = new JSONObject();
        final JSONObject metaObject = new JSONObject();
        
        // if no stats were entered, figure out what type we are dealing with and select out default stats. 
        if (metricData.getData().getPoints().size() > 0 && filterStats == PlotRequestParser.DEFAULT_STATS) {
            Class dataClass = metricData.getData().getDataClass();
            if (dataClass.equals(BasicRollup.class))
                filterStats = PlotRequestParser.DEFAULT_BASIC;
            else if (dataClass.equals(BluefloodGaugeRollup.class))
                filterStats = PlotRequestParser.DEFAULT_GAUGE;
            else if (dataClass.equals(BluefloodCounterRollup.class))
                filterStats = PlotRequestParser.DEFAULT_COUNTER;
            else if (dataClass.equals(BluefloodSetRollup.class))
                filterStats = PlotRequestParser.DEFAULT_SET;
            else if (dataClass.equals(BluefloodTimerRollup.class))
                filterStats = PlotRequestParser.DEFAULT_TIMER;
            // else, I got nothing.
        }

        final JSONArray valuesArray = transformDataToJSONArray(metricData, filterStats);

        metaObject.put("count", valuesArray.size());
        metaObject.put("limit", null);
        metaObject.put("marker", null);
        metaObject.put("next_href", null);
        globalJSON.put("values", valuesArray);
        globalJSON.put("metadata", metaObject);
        globalJSON.put("unit", metricData.getUnit() == null ? Util.UNKNOWN : metricData.getUnit());

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
        
        
        
        // todo: adding getCount() to Rollup interface will simplify this block.
        // because of inheritance, GaugeRollup needs to come before BasicRollup. sorry.
        if (point.getData() instanceof BluefloodGaugeRollup) {
            BluefloodGaugeRollup rollup = (BluefloodGaugeRollup)point.getData();
            numPoints += rollup.getCount();
            filterStatsObject = getFilteredStatsForRollup(rollup, filterStats);
        } else if (point.getData() instanceof BasicRollup) {
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
        } else if (point.getData() instanceof BluefloodSetRollup) {
            BluefloodSetRollup rollup = (BluefloodSetRollup)point.getData();
            numPoints += rollup.getCount();
            filterStatsObject = getFilteredStatsForRollup(rollup, filterStats);
        } else if (point.getData() instanceof BluefloodTimerRollup) {
            BluefloodTimerRollup rollup = (BluefloodTimerRollup)point.getData();
            numPoints += rollup.getCount();
            filterStatsObject = getFilteredStatsForRollup(rollup, filterStats);
        } else if (point.getData() instanceof BluefloodCounterRollup) {
            BluefloodCounterRollup rollup = (BluefloodCounterRollup)point.getData();
            numPoints += rollup.getCount().longValue();
            filterStatsObject = getFilteredStatsForRollup(rollup, filterStats);
        } else {
            String errString =
              String.format("Unsupported datatype for Point %s",
                point.getData().getClass());
            log.error(errString);
            throw new SerializationException(errString);
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

    private JSONObject getFilteredStatsForRollup(Rollup rollup, Set<MetricStat> filterStats) {
        final JSONObject filteredObject = new JSONObject();
        for (MetricStat stat : filterStats) {
            try {
                Object filteredValue = stat.convertRollupToObject(rollup);
                if (filteredValue instanceof Map && stat == MetricStat.PERCENTILE) {
                    for (Map.Entry entry : ((Map<?,?>)filteredValue).entrySet()) {
                        BluefloodTimerRollup.Percentile pct = (BluefloodTimerRollup.Percentile)entry.getValue();
                        filteredObject.put(String.format("pct_%s", entry.getKey().toString()), pct.getMean());
                    }
                } else {
                    filteredObject.put(stat.toString(), filteredValue);
                }
            } catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            }
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
