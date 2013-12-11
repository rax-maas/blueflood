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


import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.types.HistogramRollup;
import com.rackspacecloud.blueflood.types.Points;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;

public class JSONHistogramOutputSerializer {

    public JSONObject transformHistogram(MetricData data) throws SerializationException {
        final JSONObject globalJSON = new JSONObject();
        final JSONObject metaObject = new JSONObject();

        final JSONArray valuesArray = transformDataToJSONArray(data);

        metaObject.put("count", valuesArray.size());
        metaObject.put("limit", null);
        metaObject.put("marker", null);
        metaObject.put("next_href", null);
        globalJSON.put("values", valuesArray);
        globalJSON.put("metadata", metaObject);

        return globalJSON;
    }

    private JSONArray transformDataToJSONArray(MetricData metricData) throws SerializationException {
        Points points = metricData.getData();
        final JSONArray data = new JSONArray();
        final Set<Map.Entry<Long, Points.Point>> dataPoints = points.getPoints().entrySet();
        for (Map.Entry<Long, Points.Point> point : dataPoints) {
            data.add(toJSON(point.getKey(), point.getValue(), metricData.getUnit()));
        }

        return data;
    }

    private JSONObject toJSON(long timestamp, Points.Point point, String unit) throws SerializationException {
        final JSONObject object = new JSONObject();
        object.put("timestamp", timestamp);

        if (!(point.getData() instanceof HistogramRollup)) {
            throw new SerializationException("Unsupported type. HistogramRollup expected.");
        }

        HistogramRollup histogramRollup = (HistogramRollup) point.getData();

        final JSONArray hist = new JSONArray();
        for (Bin<SimpleTarget> bin : histogramRollup.getBins()) {
            final JSONObject obj = new JSONObject();
            obj.put("mean", bin.getMean());
            obj.put("count", bin.getCount());
            hist.add(obj);
        }
        object.put("histogram", hist);

        return object;
    }
}
