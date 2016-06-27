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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.BasicRollup;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_FULL_RES;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_2_ROLLUP;

/**
 * This class knows how to serialize/deserialize a BasicRollup to its byte
 * wire format.
 */
public class BasicRollupSerDes extends BaseRollupSerDes {

    @VisibleForTesting
    public ByteBuffer serializeV1( BasicRollup basicRollup ) {

        try {
            byte[] buf = new byte[sizeOf( basicRollup, VERSION_1_ROLLUP)];
            CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
            serializeRollupV1( basicRollup, protobufOut );
            return ByteBuffer.wrap(buf);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

    }

    public ByteBuffer serialize(BasicRollup basicRollup) {
        try {
            byte[] buf = new byte[sizeOf(basicRollup, VERSION_2_ROLLUP)];
            serializeRollup( basicRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BasicRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_FULL_RES && version != VERSION_1_ROLLUP && version != VERSION_2_ROLLUP) {
                throw new SerializationException(String.format("Unexpected serialization version: %d",
                        (int)version));
            }

            return deserializeRollup( in, version );

        } catch (Exception e) {
            throw new RuntimeException("Deserialization Failure", e);
        }
    }

    protected BasicRollup deserializeRollup(CodedInputStream in, byte version) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();

        deserializeBaseRollup( basicRollup, in, version );

        if( version == VERSION_2_ROLLUP ) {
            basicRollup.setSum( in.readDouble() );
        }

        return basicRollup;
    }

    protected int sizeOf(BasicRollup basicRollup, byte version) {

        int sz = sizeOfBaseRollup( basicRollup );
        if( version == VERSION_2_ROLLUP )
            sz += CodedOutputStream.computeDoubleSizeNoTag(basicRollup.getSum());

        return sz;
    }

    protected void serializeRollup(BasicRollup basicRollup, byte[] buf) throws IOException {
        rollupSize.update(buf.length);
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        protobufOut.writeRawByte(Constants.VERSION_2_ROLLUP);

        serializeBaseRollupHelper( basicRollup, protobufOut );

        if (basicRollup.getCount() > 0) {
            protobufOut.writeDoubleNoTag( basicRollup.getSum() );
        }
    }
}
