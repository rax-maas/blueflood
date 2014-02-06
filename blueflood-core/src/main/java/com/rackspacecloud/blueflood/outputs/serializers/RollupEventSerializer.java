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

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.outputs.serializers.helpers.RollupSerializationHelper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;

public class RollupEventSerializer {
    private static final Logger log = LoggerFactory.getLogger(RollupEventSerializer.class);

    public static ObjectNode serializeRollupEvent(RollupEvent rollupPayload) {
        //Metadata Node
        ObjectNode metaNode = JsonNodeFactory.instance.objectNode();
        metaNode.put("type", rollupPayload.getRollup().getRollupType().toString());
        metaNode.put("unit", rollupPayload.getUnit());
        //Create and fill up root node
        ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
        try {
            rootNode.put("tenantId", rollupPayload.getLocator().getTenantId());
            rootNode.put("metricName", rollupPayload.getLocator().getMetricName());
            rootNode.put("gran", rollupPayload.getGranularityName());
            rootNode.put("rollup", RollupSerializationHelper.rollupToJson(rollupPayload.getRollup()));
            rootNode.put("timestamp", rollupPayload.getTimestamp());
            rootNode.put("metadata", metaNode);
        } catch (Exception e) {
            log.error("Error encountered while serializing rollup. Locator:" + rollupPayload.getLocator() + " timestamp:" + System.currentTimeMillis(), e);
            throw new IOError(e);
        }
        return rootNode;
    }
}
