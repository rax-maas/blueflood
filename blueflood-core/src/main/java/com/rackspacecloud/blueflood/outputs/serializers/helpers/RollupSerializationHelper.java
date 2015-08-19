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

package com.rackspacecloud.blueflood.outputs.serializers.helpers;

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.rackspacecloud.blueflood.types.*;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;

public class RollupSerializationHelper {
    private static final Logger log = LoggerFactory.getLogger(RollupSerializationHelper.class);

    public static ObjectNode rollupToJson(Rollup rollup) {
        if (rollup instanceof BluefloodCounterRollup)
            return handleCounterRollup((BluefloodCounterRollup)rollup);
        else if (rollup instanceof BluefloodTimerRollup)
            return handleTimerRollup((BluefloodTimerRollup)rollup);
        else if (rollup instanceof BluefloodSetRollup)
            return handleSetRollup((BluefloodSetRollup)rollup);
        else if (rollup instanceof BluefloodGaugeRollup)
            return handleGaugeRollup((BluefloodGaugeRollup)rollup);
        else if (rollup instanceof BasicRollup)
            return handleBasicRollup((BasicRollup)rollup, JsonNodeFactory.instance.objectNode());
        else if (rollup instanceof HistogramRollup)
            return handleHistogramRollup((HistogramRollup)rollup);
        else {
            log.error("Error encountered while serializing the rollup "+rollup);
            throw new IOError(new IOException("Cannot serialize the Rollup : "+rollup));
        }
    }

    private static ObjectNode handleHistogramRollup(HistogramRollup rollup) {
        ObjectNode rollupNode =  JsonNodeFactory.instance.objectNode();
        ArrayNode binArray = JsonNodeFactory.instance.arrayNode();
        Collection<Bin<SimpleTarget>> bins = rollup.getBins();
        for (Bin<SimpleTarget> bin : bins) {
            ObjectNode binNode = JsonNodeFactory.instance.objectNode();
            binNode.put("count", bin.getCount());
            binNode.put("mean", bin.getMean());
            binArray.add(binNode);
        }
        rollupNode.put("bins", binArray);
        rollupNode.put("binCount", rollup.getCount());
        return rollupNode;
    }

    private static ObjectNode handleBasicRollup(IBasicRollup rollup, ObjectNode rollupNode) {
        long count = rollup.getCount();
        rollupNode.put("count", count);
        if (count == 0) {
            rollupNode.putNull("max");
            rollupNode.putNull("min");
            rollupNode.putNull("mean");
            rollupNode.putNull("var");
        } else {
            rollupNode.put("max", rollup.getMaxValue().isFloatingPoint() ? rollup.getMaxValue().toDouble() : rollup.getMaxValue().toLong());
            rollupNode.put("min", rollup.getMinValue().isFloatingPoint() ? rollup.getMinValue().toDouble() : rollup.getMinValue().toLong());
            rollupNode.put("mean", rollup.getAverage().isFloatingPoint() ? rollup.getAverage().toDouble() : rollup.getAverage().toLong());
            rollupNode.put("var", rollup.getVariance().isFloatingPoint() ? rollup.getVariance().toDouble() : rollup.getVariance().toLong());
        }
        return rollupNode;
    }

    private static ObjectNode handleGaugeRollup(BluefloodGaugeRollup rollup) {
        ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
        SimpleNumber rollupValue = rollup.getLatestValue();
        rollupNode.put("latestVal", rollupValue.getDataType() == (SimpleNumber.Type.DOUBLE) ? rollupValue.getValue().doubleValue() : rollupValue.getValue().longValue());
        return handleBasicRollup(rollup, rollupNode);
    }

    private static ObjectNode handleSetRollup(BluefloodSetRollup rollup) {
        ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
        rollupNode.put("count", rollup.getCount());
        return rollupNode;
    }

    private static ObjectNode handleTimerRollup(BluefloodTimerRollup rollup) {
        ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
        rollupNode.put("sum", rollup.getSum());
        rollupNode.put("rate", rollup.getRate());
        rollupNode.put("sampleCount", rollup.getSampleCount());
        return handleBasicRollup(rollup, rollupNode);
    }

    private static ObjectNode handleCounterRollup(BluefloodCounterRollup rollup) {
        ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
        rollupNode.put("count", (rollup.getCount() instanceof Float || rollup.getCount() instanceof Double) ? rollup.getCount().doubleValue() : rollup.getCount().longValue());
        rollupNode.put("sampleCount", rollup.getSampleCount());
        rollupNode.put("rate", rollup.getRate());
        return rollupNode;
    }
}
