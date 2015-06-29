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

import org.junit.Assert;
import org.junit.Test;

public class RollupExecutionContextTest {

    @Test
    public void testExecutionContext() {
        Thread myThread = new Thread();

        RollupExecutionContext myRollupContext = new RollupExecutionContext(myThread);

        // validate read behavior
        Assert.assertTrue(myRollupContext.doneReading());
        myRollupContext.incrementReadCounter();
        Assert.assertFalse(myRollupContext.doneReading());
        myRollupContext.decrementReadCounter();
        Assert.assertTrue(myRollupContext.doneReading());

        // validate put behavior
        Assert.assertTrue(myRollupContext.doneWriting());
        myRollupContext.incrementWriteCounter();
        myRollupContext.incrementWriteCounter();
        Assert.assertFalse(myRollupContext.doneWriting());
        myRollupContext.decrementWriteCounter(2);
        Assert.assertTrue(myRollupContext.doneWriting());
    }
}
