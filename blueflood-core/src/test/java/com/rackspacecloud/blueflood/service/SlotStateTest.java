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

package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.serializers.SlotStateSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Assert;
import org.junit.Test;

public class SlotStateTest {
    private final long time = 123456;
    private final String s1 = "metrics_full,1,A";
    private final String s2 = "metrics_60m,1,A";
    private final String s3 = "metrics_full,1,X";

    private final SlotState ss1 = new SlotState(Granularity.FULL, 1, UpdateStamp.State.Active);
    private final SlotState ss2 = new SlotState(Granularity.MIN_60, 1, UpdateStamp.State.Running).withTimestamp(time);
    private final SlotState ss3 = new SlotState(Granularity.FULL, 1, UpdateStamp.State.Rolled).withTimestamp(time);

    @Test
    public void testStringConversion() {
        // verify active and running are the same string rep.
        // verify that toString includes timestamp...
        Assert.assertEquals(s1 + ": ", ss1.toString()); // ...unless it wasn't specified
        Assert.assertEquals(s2 + ": " + time, ss2.toString());
        Assert.assertEquals(s3 + ": " + time, ss3.toString());
    }

    @Test
    public void testEquality() {
        // verify that equality works with and without timestamp
        Assert.assertEquals(ss1, fromString(s1));
        Assert.assertEquals(ss2, fromString(s2).withTimestamp(time));
        // verify that Active and Running are considered equal
        Assert.assertEquals(new SlotState(Granularity.FULL, 1, UpdateStamp.State.Active),
                new SlotState(Granularity.FULL, 1, UpdateStamp.State.Running));
        // ... but that they are not equal to Rolled
        Assert.assertNotSame(new SlotState(Granularity.FULL, 1, UpdateStamp.State.Active),
                new SlotState(Granularity.FULL, 1, UpdateStamp.State.Rolled));
        // verify that inequality works
        SlotState timestampedState = fromString(s1).withTimestamp(time);
        Assert.assertNotSame(timestampedState, fromString(s1));
    }

    @Test
    public void testGranularity() {
        Assert.assertEquals(Granularity.FULL, fromString(s1).getGranularity());
        Assert.assertNull(fromString("FULL,1,X").getGranularity());
    }

    private SlotState fromString(String string) {
        SlotStateSerializer slotSer = SlotStateSerializer.get();
        StringSerializer stringSer = StringSerializer.get();
        return slotSer.fromByteBuffer(stringSer.toByteBuffer(string));
    }
}
