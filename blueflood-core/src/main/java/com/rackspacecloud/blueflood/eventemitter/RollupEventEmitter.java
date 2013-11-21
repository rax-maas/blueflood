package com.rackspacecloud.blueflood.eventemitter;

import com.github.nkzawa.emitter.Emitter;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.Locator;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupEventEmitter {

    private static Emitter instance = new Emitter();
    private static final Logger log = LoggerFactory.getLogger(RollupEventEmitter.class);


    public static Emitter getEmitterInstance() {
        return instance;
    }


    //TODO: Generalize this later
    public static void emitAsJSON (String event, Locator loc, Rollup rollup, String unit) {
      BasicRollup roll;
      if(rollup instanceof BasicRollup) {
        roll = (BasicRollup) rollup;
      } else {
        log.error("Expected basic rollup but received "+rollup);
        return;
      }
      //Main container node
      ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
      //Rollup node
      ObjectNode rollupNode = JsonNodeFactory.instance.objectNode();
      rollupNode.put("maxValue", roll.getMaxValue().toDouble());
      rollupNode.put("minValue", roll.getMinValue().toDouble());
      rollupNode.put("average", roll.getAverage().toDouble());
      rollupNode.put("variance", roll.getVariance().toDouble());
      rollupNode.put("count", roll.getCount());
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
        rootNode.put("locator", loc.getTenantId() + "." + loc.getMetricName());
        rootNode.put("rollup", rollupNode);
        rootNode.put("metadata", metaNode);
      } catch (Exception e) {
        log.error("Error encountered while serializing rollup");
        return;
      }
      //Emit an stringified JSON object
      instance.emit(event, rootNode.toString());
    }

}
