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

import com.codahale.metrics.Histogram;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.BluefloodCounterRollup;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_COUNTER_ROLLUP;

/**
 * This class knows how to serialize/deserialize counter objects.
 */
public class CounterSerDes extends AbstractSerDes {

    private static Histogram counterRollupSize = Metrics.histogram(CounterSerDes.class, "Counter Gauge Metric Size");

    public ByteBuffer serialize(BluefloodCounterRollup counterRollup) {
        try {
            byte[] buf = new byte[sizeOf(counterRollup)];
            serializeCounterRollup(counterRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public BluefloodCounterRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_COUNTER_ROLLUP)
                throw new SerializationException(String.format("Unexpected counter deserialization version: %d", (int)version));
            return deserializeV1CounterRollup(in);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private int sizeOf(BluefloodCounterRollup counterRollup) {
        int sz = sizeOfSize();
        sz += sizeOfType();
        if (counterRollup.getCount() instanceof Long || counterRollup.getCount() instanceof Integer)
            sz += CodedOutputStream.computeRawVarint64Size(counterRollup.getCount().longValue());
        else if (counterRollup.getCount() instanceof Double || counterRollup.getCount() instanceof Float)
            sz += CodedOutputStream.computeDoubleSizeNoTag(counterRollup.getCount().doubleValue());
        sz += CodedOutputStream.computeDoubleSizeNoTag(counterRollup.getRate());
        sz += CodedOutputStream.computeRawVarint32Size(counterRollup.getSampleCount());
        return sz;
    }

    private void serializeCounterRollup(BluefloodCounterRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        counterRollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_COUNTER_ROLLUP);
        putUnversionedDoubleOrLong(rollup.getCount(), out);
        out.writeDoubleNoTag(rollup.getRate());
        out.writeRawVarint32(rollup.getSampleCount());
    }

    private BluefloodCounterRollup deserializeV1CounterRollup(CodedInputStream in) throws IOException {
        Number value = getUnversionedDoubleOrLong(in);
        double rate = in.readDouble();
        int sampleCount = in.readRawVarint32();
        return new BluefloodCounterRollup().withCount(value.longValue()).withRate(rate).withSampleCount(sampleCount);
    }

}
