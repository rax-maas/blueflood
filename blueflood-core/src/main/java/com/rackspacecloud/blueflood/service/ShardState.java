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
import com.rackspacecloud.blueflood.utils.Util;

public class ShardState {
    private Granularity granularity;
    private int slot;
    private UpdateStamp.State state;
    private String stringRep;

    public ShardState(String compoundShardState) {
        this.granularity = Util.granularityFromStateCol(compoundShardState);
        this.slot = Util.slotFromStateCol(compoundShardState);
        this.state = stateFromCode(Util.stateFromStateCol(compoundShardState));
        this.stringRep = calculateStringRep();
    }

    private UpdateStamp.State stateFromCode(String stateCode) {
        if (stateCode.equals(UpdateStamp.State.Rolled.code())) {
            return UpdateStamp.State.Rolled;
        } else {
            return UpdateStamp.State.Active;
        }
    }

    private String calculateStringRep() {
        return new StringBuilder()
                .append(granularity.name()).append(",")
                .append(slot).append(",")
                .append(state.code()).append(",")
                .toString();
    }

    public String toString() {
        return stringRep;
    }
}
