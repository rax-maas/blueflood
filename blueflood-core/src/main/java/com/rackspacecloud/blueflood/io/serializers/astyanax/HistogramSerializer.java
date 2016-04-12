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
import com.rackspacecloud.blueflood.io.serializers.metrics.HistogramSerDes;
import com.rackspacecloud.blueflood.types.HistogramRollup;

import java.nio.ByteBuffer;

public class HistogramSerializer extends AbstractSerializer<HistogramRollup> {
    private static final HistogramSerDes deSer = new HistogramSerDes();
    private static final HistogramSerializer INSTANCE = new HistogramSerializer();

    public static HistogramSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(HistogramRollup histogramRollup) {
        return deSer.serialize(histogramRollup);
    }

    @Override
    public HistogramRollup fromByteBuffer(ByteBuffer byteBuffer) {
        return deSer.deserialize(byteBuffer);
    }

    // prevent people from instantiating this class
    private HistogramSerializer() {
    }
}
