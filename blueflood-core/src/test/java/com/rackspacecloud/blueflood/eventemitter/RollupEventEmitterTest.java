package com.rackspacecloud.blueflood.eventemitter;

import com.github.nkzawa.emitter.Emitter;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Locator;
import junit.framework.Assert;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;



public class RollupEventEmitterTest {

    String testEventName = "test";
    List<Object> capturedTestEvents;

    BasicRollup roll = new BasicRollup();
    Locator loc = new Locator();
    String testUnitsString = "testUnits";

    EventListener elistener = new EventListener();
    EventListener mockListener = spy(elistener);

    @Test
    public void testSubscribe() {
      RollupEventEmitter.getEmitterInstance().on(testEventName,mockListener);
      Assert.assertEquals(true, RollupEventEmitter.getEmitterInstance().listeners(testEventName).contains(mockListener));
    }

    @Test
    public void testRollupEmit() {
      RollupEventEmitter.emitAsJSON(testEventName, loc, roll, testUnitsString);
    }

    @Test
    public void testUnsubscribe() {
      RollupEventEmitter.getEmitterInstance().off(testEventName, elistener);
      Assert.assertEquals(false, RollupEventEmitter.getEmitterInstance().listeners(testEventName).contains(mockListener));
    }

    private class EventListener implements Emitter.Listener {
        @Override
        public void call(Object... objects) {
            //This is stupidity. I know.
            verify(mockListener, atLeastOnce()).call(anyVararg());
            capturedTestEvents = Arrays.asList(objects);
            Assert.assertNotNull(capturedTestEvents);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode df = null;
            try {
                df = mapper.readValue((String)capturedTestEvents.get(0),JsonNode.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(df.get("metadata").get("units").get("name").getTextValue(), testUnitsString);
            Assert.assertNotNull(df.get("rollup").get("maxValue").getDoubleValue());
            Assert.assertEquals(df.get("rollup").get("maxValue").getDoubleValue(), roll.getMaxValue().toDouble());
        }
    }

}
