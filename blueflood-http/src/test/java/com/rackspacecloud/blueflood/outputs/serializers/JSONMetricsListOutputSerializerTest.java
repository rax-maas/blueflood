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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JSONMetricsListOutputSerializerTest {

    @Test
    public void testMetricsListSerialization() {
        List<ElasticIO.Result> results = new ArrayList<ElasticIO.Result>();
        results.add(new ElasticIO.Result("123456", "metric.foo", "unknown"));
        results.add(new ElasticIO.Result("123456", "agent.cpu", "percent"));

        final JSONObject json = JSONMetricsListOutputSerializer.transform(results);
        final JSONArray metrics = (JSONArray) json.get("values");
        Assert.assertEquals(results.size(), metrics.size());

        for (int i = 0; i < metrics.size(); i++) {
            JSONObject item = (JSONObject) metrics.get(i);
            Assert.assertNotNull(item.get("name"));
            Assert.assertNotNull(item.get("unit"));
        }
    }
}
