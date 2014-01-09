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

package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.UpdateStamp;
import org.junit.Assert;
import org.junit.Test;

public class SlotStateSerializerTest {
    @Test
    public void testGranularityFromStateCol() {
        Granularity myGranularity = SlotStateSerializer.granularityFromStateCol("metrics_full,1,okay");
        Assert.assertNotNull(myGranularity);
        Assert.assertEquals(myGranularity, Granularity.FULL);

        myGranularity = SlotStateSerializer.granularityFromStateCol("FULL");
        Assert.assertNull(myGranularity);
    }

    @Test
    public void testSlotFromStateCol() {
        Assert.assertEquals(1, SlotStateSerializer.slotFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCol() {
        Assert.assertEquals("okay", SlotStateSerializer.stateCodeFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCode() {
        Assert.assertEquals(UpdateStamp.State.Active, SlotStateSerializer.stateFromCode("foo"));
        Assert.assertEquals(UpdateStamp.State.Active, SlotStateSerializer.stateFromCode("A"));
        Assert.assertEquals(UpdateStamp.State.Rolled, SlotStateSerializer.stateFromCode("X"));
    }
}
