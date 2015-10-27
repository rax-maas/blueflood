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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Discovery {
    private Map<String, Object> fields = new HashMap<String, Object>();
    private final String metricName;
    private final String tenantId;

    public Discovery(String tenantId, String metricName) {
        this.tenantId = tenantId;
        this.metricName = metricName;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getDocumentId() {
        return tenantId + ":" + metricName;
    }

    @Override
    public String toString() {
        return "ElasticMetricDiscovery [" +
                "tenantId=" + tenantId + ", " +
                "metricName=" + metricName + ", " +
                "fields=" + fields.toString() + "]";
    }

    public Discovery withSourceFields(Map<String, Object> fields) {
        this.fields = fields;
        return this;
    }

    XContentBuilder createSourceContent() throws IOException {
        XContentBuilder json;

        json = XContentFactory.jsonBuilder().startObject()
                .field(ESFieldLabel.tenantId.toString(), tenantId)
                .field(ESFieldLabel.metric_name.toString(), metricName);

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            json = json.field(entry.getKey(), entry.getValue());
        }
        json = json.endObject();
        return json;
    }
}
