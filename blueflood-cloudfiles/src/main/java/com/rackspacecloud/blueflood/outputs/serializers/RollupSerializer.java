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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.commons.lang.NotImplementedException;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This file is being temporarily added. It will be replaced
 */
public class RollupSerializer {
    private static final Logger log = LoggerFactory.getLogger(RollupSerializer.class);

    public RollupEvent fromBytes(byte[] bytes) {
        //TODO :  Decide on requirements of consumer side and change accordingly
        throw new NotImplementedException("fromBytes not yet implemented.");
    }

    public byte[] toBytes(RollupEvent rollupPayload) {
        Locator locator = rollupPayload.getLocator();
        BasicRollup rollup = (BasicRollup) rollupPayload.getRollup();
        String unit = rollupPayload.getUnit();
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
        metaNode.put("unit", unit);
        metaNode.put("type", "numeric");
        //Fill up the root node
        try {
            rootNode.put("locator", locator.toString());
            rootNode.put("rollup", rollupNode);
            rootNode.put("metadata", metaNode);
        } catch (Exception e) {
            log.error("Error encountered while serializing rollup. Locator:" + locator.toString() + " timestamp:" + System.currentTimeMillis(), e);
            throw new RuntimeException("Error encountered while serializing rollup. Locator:" + locator.toString() + " timestamp:" + System.currentTimeMillis(), e);
        }
        return rootNode.toString().getBytes();
    }
}