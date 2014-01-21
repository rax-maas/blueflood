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

package com.rackspacecloud.blueflood.io.serializers;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

import java.nio.ByteBuffer;

public class SlotStateSerializer extends AbstractSerializer<SlotState> {
    private static final SlotStateSerializer INSTANCE = new SlotStateSerializer();

    public static SlotStateSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(SlotState state) {
        Granularity gran = state.getGranularity();
        String stringRep = new StringBuilder().append(gran == null ? "null" : gran.name())
                .append(",").append(state.getSlot())
                .append(",").append(state == null ? "null" : state.getState().code())
                .toString();

        return StringSerializer.get().toByteBuffer(stringRep);
    }

    @Override
    public SlotState fromByteBuffer(ByteBuffer byteBuffer) {
        String stringRep = StringSerializer.get().fromByteBuffer(byteBuffer);
        Granularity g = granularityFromStateCol(stringRep);
        Integer slot = slotFromStateCol(stringRep);
        UpdateStamp.State state = stateFromCode(stateCodeFromStateCol(stringRep));

        return new SlotState(g, slot, state);
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
