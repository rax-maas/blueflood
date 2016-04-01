/*
 * Copyright 2016 Rackspace
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
package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

/**
 * This class knows how to serialize/deserialize the SlotState objects.
 */
public class SlotStateSerDes {


    public static SlotState deserialize(String stateStr) {
        Granularity g = granularityFromStateCol(stateStr);
        Integer slot = slotFromStateCol(stateStr);
        UpdateStamp.State state = stateFromCode(stateCodeFromStateCol(stateStr));
        return new SlotState(g, slot, state);
    }

    public String serialize(SlotState state) {
        return serialize(state.getGranularity(), state.getSlot(), state.getState());
    }

    public String serialize(Granularity gran, int slot, UpdateStamp.State state) {
        String stringRep = new StringBuilder().append(gran == null ? "null" : gran.name())
                .append(",").append(slot)
                .append(",").append(state == null ? "null" : state.code())
                .toString();
        return stringRep;
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
