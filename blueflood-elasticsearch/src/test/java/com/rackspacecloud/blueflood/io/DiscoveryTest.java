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

package com.rackspacecloud.blueflood.io;

import junit.framework.Assert;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DiscoveryTest {

    final String TENANT_1 = "987654";
    final String METRIC_NAME_A = "metric.a.b.c.d.1";

    @Test
    public void testCreateDiscovery() {
        Discovery discovery = new Discovery(TENANT_1, METRIC_NAME_A);
        Assert.assertEquals(METRIC_NAME_A, discovery.getMetricName());
        Assert.assertEquals(TENANT_1, discovery.getTenantId());
        Assert.assertEquals(TENANT_1 + ":" + METRIC_NAME_A, discovery.getDocumentId());
        Assert.assertNotNull(discovery.toString());
    }

    @Test
    public void testWithAnnotation() {
        Map<String, Object> info = new HashMap<String, Object>();
        info.put("a_1", "a1");
        info.put("a_2", "a2");
        info.put("a_3", "a3");

        Discovery discovery = new Discovery(TENANT_1, METRIC_NAME_A).withAnnotation(info);

        Map<String, Object> actualInfo = discovery.getAnnotation();
        Assert.assertEquals("a1", actualInfo.get("a_1").toString());
        Assert.assertEquals("a2", actualInfo.get("a_2").toString());
        Assert.assertEquals("a3", actualInfo.get("a_3").toString());
    }

    @Test
    public void testCreateSourceContent() throws IOException {

        Map<String, Object> info = new HashMap<String, Object>();
        info.put(ESFieldLabel.unit.toString(), "quantum");

        Discovery discovery = new Discovery(TENANT_1, METRIC_NAME_A).withAnnotation(info);
        XContentBuilder builder = discovery.createSourceContent();

        final String expectedString =
                "{" +
                    "\"tenantId\":\"" + TENANT_1 + "\"," +
                    "\"metric_name\":\"" + METRIC_NAME_A + "\"," +
                    "\"unit\":\"quantum\"" +
                "}";

        Assert.assertEquals(expectedString, builder.string());
    }
}
