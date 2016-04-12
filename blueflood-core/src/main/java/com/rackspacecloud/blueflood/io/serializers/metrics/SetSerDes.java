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
import com.rackspacecloud.blueflood.types.BluefloodSetRollup;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_SET_ROLLUP;

/**
 * This class knows how to serialize/deserialize Set objects.
 */
public class SetSerDes extends AbstractSerDes {

    /**
     * Our own internal metric to count the number of Set rollups
     */
    private static Histogram setRollupSize = Metrics.histogram(SetSerDes.class, "Set Metric Size");

    public ByteBuffer serialize(BluefloodSetRollup setRollup) {
        try {
            byte[] buf = new byte[sizeOf(setRollup)];
            serializeSetRollup(setRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public BluefloodSetRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_SET_ROLLUP)
                throw new SerializationException(String.format("Unexpected set serialization version: %d", (int)version));
            return deserializeV1SetRollup(in);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void serializeSetRollup(BluefloodSetRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        setRollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_SET_ROLLUP);
        out.writeRawVarint32(rollup.getCount());
        for (Integer i : rollup.getHashes()) {
            out.writeRawVarint32(i);
        }
    }

    private BluefloodSetRollup deserializeV1SetRollup(CodedInputStream in) throws IOException {
        int count = in.readRawVarint32();
        BluefloodSetRollup rollup = new BluefloodSetRollup();
        while (count-- > 0) {
            rollup = rollup.withObject(in.readRawVarint32());
        }
        return rollup;
    }

    private int sizeOf(BluefloodSetRollup setRollup) {
        int sz = sizeOfSize();
        sz += CodedOutputStream.computeRawVarint32Size(setRollup.getCount());
        for (Integer i : setRollup.getHashes()) {
            sz += CodedOutputStream.computeRawVarint32Size(i);
        }
        return sz;
    }
}
