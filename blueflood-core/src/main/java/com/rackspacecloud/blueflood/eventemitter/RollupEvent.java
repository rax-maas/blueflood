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

package com.rackspacecloud.blueflood.eventemitter;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;

public class RollupEvent {
    private final Locator locator;
    private final Rollup rollup;
    private String unit;
    private final String granularityName;
    //Rollup slot in millis
    private final long timestamp;

    public RollupEvent(Locator loc, Rollup rollup, String unit, String gran, long ts) {
        this.locator = loc;
        this.rollup = rollup;
        this.unit = unit;
        this.granularityName = gran;
        this.timestamp = ts;
    }

    public Rollup getRollup() {
        return rollup;
    }

    public Locator getLocator() {
        return locator;
    }

    public String getUnit() {
        return unit;
    }

    public String getGranularityName() {
        return granularityName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
