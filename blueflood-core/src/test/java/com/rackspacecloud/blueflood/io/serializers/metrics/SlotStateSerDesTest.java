package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by shin4590 on 3/22/16.
 */
public class SlotStateSerDesTest {

    private static SlotStateSerDes serDes = new SlotStateSerDes();

    @Test
    public void testGranularityFromStateCol() {
        Granularity myGranularity = serDes.granularityFromStateCol("metrics_full,1,okay");
        Assert.assertNotNull(myGranularity);
        Assert.assertEquals(myGranularity, Granularity.FULL);

        myGranularity = serDes.granularityFromStateCol("FULL");
        Assert.assertNull(myGranularity);
    }

    @Test
    public void testSlotFromStateCol() {
        Assert.assertEquals(1, serDes.slotFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCol() {
        Assert.assertEquals("okay", serDes.stateCodeFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCode() {
        Assert.assertEquals(UpdateStamp.State.Active, serDes.stateFromCode("foo"));
        Assert.assertEquals(UpdateStamp.State.Active, serDes.stateFromCode("A"));
        Assert.assertEquals(UpdateStamp.State.Rolled, serDes.stateFromCode("X"));
    }
}
