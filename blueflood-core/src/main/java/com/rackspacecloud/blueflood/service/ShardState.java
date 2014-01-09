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

public class ShardState {
    private Granularity granularity;
    private Integer slot;
    private UpdateStamp.State state;
    private String stringRep;
    private Long timestamp = null;

    public ShardState(String compoundShardState) {
        this.granularity = granularityFromStateCol(compoundShardState);
        this.slot = slotFromStateCol(compoundShardState);
        this.state = stateFromCode(stateCodeFromStateCol(compoundShardState));
        this.stringRep = calculateStringRep();
    }

    public ShardState(Granularity g, int slot, UpdateStamp.State state) {
        this.granularity = g;
        this.slot = slot;
        this.state = state;
        this.stringRep = calculateStringRep();
    }

    public ShardState() {
        this.granularity = null;
        this.slot = null;
        this.state = null;
        this.stringRep = "";
    }

    /**
     * Milliseconds
     * @param timestamp
     * @return
     */
    public ShardState withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Gets string representation of all non-optional fields (i.e. NOT timestamp)
     * Does not include timestamp. Use toString for that.
     * @return string
     */
    public String getStringRep() {
        return stringRep;
    }

    public String toString() {
        return stringRep + ": " + (getTimestamp() == null ? "" : getTimestamp());
    }

    public boolean equals(Object other) {
        if (!(other instanceof ShardState)) {
            return false;
        }
        ShardState that = (ShardState) other;
        return this.toString().equals(that.toString());
    }

    private String calculateStringRep() {
        return new StringBuilder().append(granularity == null ? "null" : granularity.name())
                .append(",").append(slot)
                .append(",").append(state == null ? "null" : state.code())
                .toString();
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

    protected static Granularity granularityFromStateCol(String s) {
        String field = s.split(",", -1)[0];
        for (Granularity g : Granularity.granularities())
            if (g.name().startsWith(field))
                return g;
        return null;
    }

    protected static int slotFromStateCol(String s) { return Integer.parseInt(s.split(",", -1)[1]); }
    protected static String stateCodeFromStateCol(String s) { return s.split(",", -1)[2]; }

    protected static UpdateStamp.State stateFromCode(String stateCode) {
        if (stateCode.equals(UpdateStamp.State.Rolled.code())) {
            return UpdateStamp.State.Rolled;
        } else {
            return UpdateStamp.State.Active;
        }
    }
}
