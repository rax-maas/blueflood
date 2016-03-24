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
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_FULL_RES;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;

/**
 * This class knows how to serialize/deserialize primitives and raw metrics.
 */
public class RawSerDes extends AbstractSerDes {

    private static Histogram fullResSize = Metrics.histogram(RawSerDes.class, "Full Resolution Metric Size");

    public ByteBuffer serialize(Object obj) {
        try {
            byte[] buf = new byte[sizeOf(obj)];

            serializeFullResMetric(obj, buf);

            ByteBuffer out = ByteBuffer.wrap(buf);
            return out;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Object deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_FULL_RES && version != VERSION_1_ROLLUP) {
                throw new SerializationException(String.format("Unexpected serialization version: %d",
                                                                (int)version));
            }
            return deserializeSimpleMetric(in);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization Failure", e);
        }
    }

    private int sizeOf(Object obj) throws SerializationException {
        int sz = sizeOfSize();
        sz += sizeOfType();

        if ( obj instanceof Integer ) {
            sz += CodedOutputStream.computeRawVarint32Size((Integer) obj);
        } else if ( obj instanceof Long ) {
            sz += CodedOutputStream.computeRawVarint64Size((Long)obj);
        } else if ( obj instanceof Double ) {
            sz += CodedOutputStream.computeDoubleSizeNoTag((Double)obj);
        } else if ( obj instanceof Float ) {
            sz += CodedOutputStream.computeDoubleSizeNoTag(((Float)obj).doubleValue());
        } else {
            throw new SerializationException("Unexpected type: " + obj.getClass().getName());
        }
        return sz;
    }

    private void serializeFullResMetric(Object obj, byte[] buf) throws IOException {
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);

        fullResSize.update(sizeOf(obj));

        protobufOut.writeRawByte(Constants.VERSION_1_FULL_RES);

        if ( obj instanceof Integer ) {
            protobufOut.writeRawByte(Constants.B_I32);
            protobufOut.writeRawVarint32((Integer) obj);
        } else if ( obj instanceof Long ) {
            protobufOut.writeRawByte(Constants.B_I64);
            protobufOut.writeRawVarint64((Long) obj);
        } else if ( obj instanceof Double ) {
            protobufOut.writeRawByte(Constants.B_DOUBLE);
            protobufOut.writeDoubleNoTag((Double) obj);
        } else if ( obj instanceof Float ) {
            protobufOut.writeRawByte(Constants.B_DOUBLE);
            protobufOut.writeDoubleNoTag(((Float) obj).doubleValue());
        } else {
            throw new SerializationException(String.format("Cannot serialize %s", obj.getClass().getName()));
        }
    }

    private Object deserializeSimpleMetric(CodedInputStream in) throws IOException {
        byte metricValueType = in.readRawByte() /* type field */;
        switch (metricValueType) {
            case Constants.I32:
                return in.readRawVarint32();
            case Constants.I64:
                return in.readRawVarint64();
            case Constants.DOUBLE:
                return in.readDouble();
            case Constants.STR:
                throw new UnexpectedStringSerializationException("We don't rollup strings");
            default:
                throw new SerializationException(String.format("Unexpected raw metric type=%s for full res " +
                                                                "metric", (char)metricValueType));
        }
    }

}
