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

package com.rackspacecloud.blueflood.kafkaserializers;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import kafka.serializer.Encoder;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupEventSerializer implements Encoder<RollupEvent>, Decoder<RollupEvent> {
    private static final Logger log = LoggerFactory.getLogger(RollupEventSerializer.class);

    public RollupEventSerializer(VerifiableProperties properties) {

    }

    @Override
    public RollupEvent fromBytes(byte[] bytes) {
        //TODO :  Decide on requirements of consumer side and change accordingly
        return null;
    }

    @Override
    public byte[] toBytes(RollupEvent rollupPayload) {
        Locator locator = rollupPayload.getLocator();
        BasicRollup rollup = (BasicRollup) rollupPayload.getRollup();
        String unit = rollupPayload.getUnits();
        //Main container node
        ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
        //Rollup node
        ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
        rollupNode.put("maxValue", rollup.getMaxValue().toDouble());
        rollupNode.put("minValue", rollup.getMinValue().toDouble());
        rollupNode.put("average", rollup.getAverage().toDouble());
        rollupNode.put("variance", rollup.getVariance().toDouble());
        rollupNode.put("count", rollup.getCount());
        //Metadata Node
        ObjectNode metaNode = JsonNodeFactory.instance.objectNode();
        //Units Node
        ObjectNode unitsNode = JsonNodeFactory.instance.objectNode();
        unitsNode.put("name", unit);
        unitsNode.put("type", "numeric");
        //Add units node to metadata node
        metaNode.put("units",unitsNode);
        //Fill up the root node
        try {
            rootNode.put("locator", locator.toString());
            rootNode.put("rollup", rollupNode);
            rootNode.put("metadata", metaNode);
        } catch (Exception e) {
            log.error("Error encountered while serializing rollup");
            return new byte[0];
        }
        return rootNode.toString().getBytes();
    }
}
