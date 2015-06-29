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

package com.rackspacecloud.blueflood.types;

import org.junit.Assert;
import org.junit.Test;

public class SimpleNumberTest {
    @Test
    public void testSimpleNumberWithVariousTypes() {
        Object testValue = new Integer(4);
        SimpleNumber simpleNumber = new SimpleNumber(testValue);
        Assert.assertEquals(testValue, simpleNumber.getValue());

        testValue = new Double(5.0);
        simpleNumber = new SimpleNumber(testValue);
        Assert.assertEquals(testValue, simpleNumber.getValue());

        testValue = new Long(5L);
        simpleNumber = new SimpleNumber(testValue);
        Assert.assertEquals(testValue, simpleNumber.getValue());

        // make sure primitives work too
        testValue = 4;
        simpleNumber = new SimpleNumber(testValue);
        Assert.assertEquals(testValue, simpleNumber.getValue());
    }

}
