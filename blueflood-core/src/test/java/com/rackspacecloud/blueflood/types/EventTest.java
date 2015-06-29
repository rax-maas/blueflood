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

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class EventTest {
    Event event = new Event();

    @Before
    public void setUp() {
        event.setWhen(1);
        event.setData("2");
        event.setTags("3");
        event.setWhat("4");
    }

    @Test
    public void testSettersGetters() {
        Assert.assertEquals(event.getWhen(), 1);
        Assert.assertEquals(event.getData(), "2");
        Assert.assertEquals(event.getTags(), "3");
        Assert.assertEquals(event.getWhat(), "4");
    }

    @Test
    public void testConvertToMap() {
        Map<String, Object> properties = event.toMap();

        Assert.assertEquals(properties.get(Event.FieldLabels.when.name()), 1L);
        Assert.assertEquals(properties.get(Event.FieldLabels.data.name()), "2");
        Assert.assertEquals(properties.get(Event.FieldLabels.tags.name()), "3");
        Assert.assertEquals(properties.get(Event.FieldLabels.what.name()), "4");
    }

    @Test
    public void testStringConstants() {
        Assert.assertEquals(Event.fromParameterName, "from");
        Assert.assertEquals(Event.untilParameterName, "until");
        Assert.assertEquals(Event.tagsParameterName, "tags");

    }
}
