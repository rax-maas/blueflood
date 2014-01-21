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

public class SlotState {
    private Granularity granularity;
    private Integer slot;
    private UpdateStamp.State state;
    private Long timestamp = null;

    public SlotState(Granularity g, int slot, UpdateStamp.State state) {
        this.granularity = g;
        this.slot = slot;
        this.state = state;
    }

    public SlotState() {
        this.granularity = null;
        this.slot = null;
        this.state = null;
    }

    /**
     * Set the timestamp
     * @param timestamp in milliseconds
     * @return
     */
    public SlotState withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String toString() {
        return new StringBuilder().append(granularity == null ? "null" : granularity.name())
                .append(",").append(slot)
                .append(",").append(state == null ? "null" : state.code())
                .append(": ").append(getTimestamp() == null ? "" : getTimestamp())
                .toString();
    }

    public boolean equals(Object other) {
        if (!(other instanceof SlotState)) {
            return false;
        }
        SlotState that = (SlotState) other;
        return this.toString().equals(that.toString());
    }

    public Granularity getGranularity() {
        return granularity;
    }

    public int getSlot() {
        return slot;
    }

    public UpdateStamp.State getState() {
        return state;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
