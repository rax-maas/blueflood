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

import com.rackspacecloud.blueflood.rollup.Granularity;
import org.junit.Assert;
import org.junit.Test;

public class ShardStateTest {
    private final long time = 123456;
    private final String s1 = "metrics_full,1,A";
    private final String s2 = "metrics_60m,1,A";
    private final String s3 = "metrics_full,1,X";

    private final ShardState ss1 = new ShardState(Granularity.FULL, 1, UpdateStamp.State.Active);
    private final ShardState ss2 = new ShardState(Granularity.MIN_60, 1, UpdateStamp.State.Running).withTimestamp(time);
    private final ShardState ss3 = new ShardState(Granularity.FULL, 1, UpdateStamp.State.Rolled).withTimestamp(time);

    @Test
    public void testStringConversion() {
        // verify active and running are the same string rep.
        // verify that timestamp is not shown for getStringRep.
        Assert.assertEquals(s1, ss1.getStringRep());
        Assert.assertEquals(s2, ss2.getStringRep());
        Assert.assertEquals(s3, ss3.getStringRep());

        // verify that toString includes timestamp...
        Assert.assertEquals(s1 + ": ", ss1.toString()); // ...unless it wasn't specified
        Assert.assertEquals(s2 + ": " + time, ss2.toString());
        Assert.assertEquals(s3 + ": " + time, ss3.toString());
    }

    @Test
    public void testEquality() {
        // verify that equality works regardless of constructor choice
        Assert.assertEquals(ss1, new ShardState(ss1.getStringRep()));
        Assert.assertEquals(ss2, new ShardState(ss2.getStringRep()).withTimestamp(time));
        // verify that Active and Running are considered equal
        Assert.assertEquals(new ShardState(Granularity.FULL, 1, UpdateStamp.State.Active),
                new ShardState(Granularity.FULL, 1, UpdateStamp.State.Running));
        // ... but that they are not equal to Rolled
        Assert.assertNotSame(new ShardState(Granularity.FULL, 1, UpdateStamp.State.Active),
                new ShardState(Granularity.FULL, 1, UpdateStamp.State.Rolled));
        // verify that inequality works
        Assert.assertNotSame(new ShardState(ss1.toString()).withTimestamp(123l), new ShardState(ss1.toString()));
    }

    @Test
    public void testGranularity() {
        Assert.assertEquals(Granularity.FULL, new ShardState(s1).getGranularity());
        Assert.assertNull(new ShardState("FULL,1,X").getGranularity());

        Granularity myGranularity = ShardState.granularityFromStateCol("metrics_full,1,okay");
        Assert.assertNotNull(myGranularity);
        Assert.assertEquals(myGranularity, Granularity.FULL);

        myGranularity = ShardState.granularityFromStateCol("FULL");
        Assert.assertNull(myGranularity);
    }

    @Test
    public void testSlotFromStateCol() {
        Assert.assertEquals(1, ShardState.slotFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCol() {
        Assert.assertEquals("okay", ShardState.stateCodeFromStateCol("metrics_full,1,okay"));
    }

    @Test
    public void testStateFromStateCode() {
        Assert.assertEquals(UpdateStamp.State.Active, ShardState.stateFromCode("foo"));
        Assert.assertEquals(UpdateStamp.State.Active, ShardState.stateFromCode("A"));
        Assert.assertEquals(UpdateStamp.State.Rolled, ShardState.stateFromCode("X"));
    }
}
