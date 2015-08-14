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

import com.rackspacecloud.blueflood.types.BluefloodGaugeRollup;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;

import java.io.IOException;

public class GaugeRollupDeserializer extends org.codehaus.jackson.map.JsonDeserializer<BluefloodGaugeRollup> {
    @Override
    public BluefloodGaugeRollup deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Integer count = 0;
        Long timestamp = null;
        Number latest = null;

        BluefloodGaugeRollup g = new BluefloodGaugeRollup();

        String fieldName = "";
        while (!jp.getCurrentToken().equals(JsonToken.END_OBJECT)) {
            if (jp.getCurrentToken().equals(JsonToken.FIELD_NAME)) {
                fieldName = jp.getText();
            }
            jp.nextToken();

            if (fieldName.equals("type")) {
                if (!jp.getText().equals("gauge")) {
                    throw new IOException("Gauge rollup must have type of 'gauge'.");
                }
            } else if (fieldName.equals("mean")) {
                g.setAverage(jp.getNumberValue());
            } else if (fieldName.equals("var")) {
                g.setVariance(jp.getNumberValue());
            } else if (fieldName.equals("min")) {
                g.setMin(jp.getNumberValue());
            } else if (fieldName.equals("max")) {
                g.setMax(jp.getNumberValue());
            } else if (fieldName.equals("count")) {
                count = jp.getIntValue();
            } else if (fieldName.equals("timestamp")) {
                timestamp = jp.getLongValue();
            } else if (fieldName.equals("latestNumericValue")) {
                latest = jp.getNumberValue();
            }

            jp.nextToken();
        }
        if (timestamp == null || latest == null || count == null) {
            if (timestamp == null)
                throw new IOException("timestamp cannot be null.");
            else if (latest == null)
                throw new IOException("latestNumericValue cannot be null.");
            else if (count == null)
                throw new IOException("count cannot be null.");
        }
        g.withLatest(timestamp, latest);
        g.setCount(count); // must come after 'withLatest' since that increments count.

        return g;
    }
}