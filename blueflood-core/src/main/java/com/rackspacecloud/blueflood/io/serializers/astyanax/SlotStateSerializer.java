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

package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.serializers.metrics.SlotStateSerDes;
import com.rackspacecloud.blueflood.service.SlotState;

import java.nio.ByteBuffer;

public class SlotStateSerializer extends AbstractSerializer<SlotState> {
    private static final SlotStateSerDes serDes = new SlotStateSerDes();
    private static final SlotStateSerializer INSTANCE = new SlotStateSerializer();

    public static SlotStateSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(SlotState state) {
        return StringSerializer.get().toByteBuffer(serDes.serialize(state));
    }

    @Override
    public SlotState fromByteBuffer(ByteBuffer byteBuffer) {
        String stringRep = StringSerializer.get().fromByteBuffer(byteBuffer);
        return serDes.deserialize(stringRep);
    }

    // prevent people from instantiating this class
    private SlotStateSerializer() {
    }
}
