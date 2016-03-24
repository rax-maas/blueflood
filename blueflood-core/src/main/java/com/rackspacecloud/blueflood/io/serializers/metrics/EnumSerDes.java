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
import com.rackspacecloud.blueflood.types.BluefloodEnumRollup;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;

/**
 * This class knows how to serialize/deserialize Enum metrics.
 */
public class EnumSerDes extends AbstractSerDes {

    /**
     * Our own internal metric to count the number of Enum rollups
     */
    private static Histogram enumRollupSize = Metrics.histogram(EnumSerDes.class, "Enum Metric Size");

    public ByteBuffer serialize(BluefloodEnumRollup enumRollup) {
        try {
            byte[] buf = new byte[sizeOf(enumRollup)];
            serializeEnum(enumRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BluefloodEnumRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_ROLLUP)
                throw new SerializationException(String.format("Unexpected enum deserialization version: %d", (int)version));
            return deserializeV1EnumRollup(in);
        } catch (Exception e) {
            throw new RuntimeException("Enum deserialization Failure", e);
        }
    }

    private int sizeOf(BluefloodEnumRollup enumRollup) {
        int sz = sizeOfSize();
        Map<Long, Long> enValues = enumRollup.getHashedEnumValuesWithCounts();
        sz += CodedOutputStream.computeRawVarint32Size(enumRollup.getCount());
        for (Long enName  : enValues.keySet()) {
            sz+=CodedOutputStream.computeRawVarint64Size(enName);
            Long enValue = enValues.get(enName);
            sz+= CodedOutputStream.computeRawVarint64Size(enValue);
        }
        return sz;
    }

    private BluefloodEnumRollup deserializeV1EnumRollup(CodedInputStream in) throws IOException {
        int count = in.readRawVarint32();
        BluefloodEnumRollup rollup = new BluefloodEnumRollup();
        while (count-- > 0) {
            rollup = rollup.withHashedEnumValue(in.readRawVarint64(), in.readRawVarint64());
        }
        return rollup;
    }

    private void serializeEnum(BluefloodEnumRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        enumRollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_ENUM_ROLLUP);
        out.writeRawVarint32(rollup.getCount());
        Map<Long, Long> enValues = rollup.getHashedEnumValuesWithCounts();
        for (Long i : enValues.keySet()) {
            out.writeRawVarint64(i);
            out.writeRawVarint64(enValues.get(i));
        }
    }
}
