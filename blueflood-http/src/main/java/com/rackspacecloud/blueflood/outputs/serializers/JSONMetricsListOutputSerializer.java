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

import com.rackspacecloud.blueflood.io.ElasticIO;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;

public class JSONMetricsListOutputSerializer {

    public static JSONObject transform(List<ElasticIO.Result> results) {
        final JSONObject global = new JSONObject();
        final JSONObject metaObject = new JSONObject();

        final JSONArray metrics = new JSONArray();
        for (ElasticIO.Result result : results) {
            final JSONObject item = new JSONObject();
            item.put("name", result.getMetricName());
            item.put("unit", result.getUnit());
            metrics.add(item);
        }

        global.put("values", metrics);
        metaObject.put("count", metrics.size());
        metaObject.put("limit", null);
        metaObject.put("marker", null);
        metaObject.put("next_href", null);
        metaObject.put("next_marker", null);
        global.put("metadata", metaObject);

        return global;
    }
}
