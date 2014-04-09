/*
 * Copyright 2014 Rackspace
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

package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MetricDeserializer extends JsonDeserializer<Metric> {
    private static final Set<String> validFields = new HashSet<String>(){{
        add("class");
        add("metricValue");
        add("collectionTime");
        add("ttlInSeconds");
        add("unit");
        add("tenantId");
        add("metricName");
    }};

    @Override
    public Metric deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Object metricValue = null;
        Long collectionTime = null;
        Long ttlInSeconds = null;
        String unit = null;
        String tenantId = null;
        String metricName = null;

        String fieldName = "";
        while (!jp.getCurrentToken().equals(JsonToken.END_OBJECT)) {

            if (jp.getCurrentToken().equals(JsonToken.FIELD_NAME)) {
                fieldName = jp.getText();
            }
            jp.nextToken();

            if (validFields.contains(fieldName)) {
                if (fieldName.equals("class")) {
                    if (!jp.getText().equals("raw")) {
                        throw new IOException("Metric.class deserialization requires field 'class' to be 'raw'");
                    }
                } else if (fieldName.equals("metricValue")) {
                    if (jp.getCurrentToken().isNumeric()) {
                        metricValue = jp.getNumberValue();
                    } else if (jp.getCurrentToken().equals(JsonToken.VALUE_FALSE)) {
                        metricValue = false;
                    } else if (jp.getCurrentToken().equals(JsonToken.VALUE_TRUE)) {
                        metricValue = true;
                    } else {
                        metricValue = jp.getText();
                    }
                } else if (fieldName.equals("collectionTime")) {
                    collectionTime = jp.getLongValue();
                } else if (fieldName.equals("ttlInSeconds")) {
                    ttlInSeconds = jp.getLongValue();
                } else if (fieldName.equals("unit")) {
                    unit = jp.getText();
                } else if (fieldName.equals("tenantId")) {
                    tenantId = jp.getText();
                } else if (fieldName.equals("metricName")) {
                    metricName = jp.getText();
                }
            }

            jp.nextToken();
        }

        if (metricValue == null || collectionTime == null || ttlInSeconds == null || unit == null || tenantId == null || metricName == null) {
            throw new IOException("A required field was not found."); // TODO: specificity
        }

        Locator locator = Locator.createLocatorFromPathComponents(tenantId, metricName);
        Metric m = new Metric(locator, metricValue, collectionTime, new TimeValue(ttlInSeconds, TimeUnit.SECONDS), unit);
        return m;
    }
}