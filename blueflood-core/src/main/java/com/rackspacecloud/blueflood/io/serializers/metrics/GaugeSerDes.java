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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.BluefloodGaugeRollup;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;

/**
 * This class knows how to serialize/deserialize Gauge metrics.
 */
public class GaugeSerDes extends BasicRollupSerDes {

    public ByteBuffer serialize(BluefloodGaugeRollup gaugeRollup) {
        try {
            byte[] buf = new byte[sizeOf(gaugeRollup)];
            serializeGauge(gaugeRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BluefloodGaugeRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_ROLLUP)
                throw new SerializationException(String.format("Unexpected gauge deserialization version: %d", (int)version));
            return deserializeV1Gauge(in);
        } catch (Exception e) {
            throw new RuntimeException("Gauge deserialization Failure", e);
        }
    }

    private int sizeOf(BluefloodGaugeRollup gaugeRollup) {
        // just like rollup up until a point.
        int sz = super.sizeOf(gaugeRollup);

        // here's where it gets different.
        sz += CodedOutputStream.computeRawVarint64Size(gaugeRollup.getTimestamp());
        sz += 1; // type of latest value.
        if (gaugeRollup.getLatestNumericValue() instanceof Long || gaugeRollup.getLatestNumericValue() instanceof Integer)
            sz += CodedOutputStream.computeRawVarint64Size(gaugeRollup.getLatestNumericValue().longValue());
        else if (gaugeRollup.getLatestNumericValue() instanceof Double || gaugeRollup.getLatestNumericValue() instanceof Float)
            sz += CodedOutputStream.computeDoubleSizeNoTag(gaugeRollup.getLatestNumericValue().doubleValue());
        return sz;
    }

    private void serializeGauge(BluefloodGaugeRollup rollup, byte[] buf) throws IOException {
        rollupSize.update(buf.length);
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        serializeRollup(rollup, protobufOut);
        protobufOut.writeRawVarint64(rollup.getTimestamp());
        putUnversionedDoubleOrLong(rollup.getLatestNumericValue(), protobufOut);
    }

    private BluefloodGaugeRollup deserializeV1Gauge(CodedInputStream in) throws IOException {
        BasicRollup basic = deserializeV1Rollup(in);
        long timestamp = in.readRawVarint64();
        Number lastValue = getUnversionedDoubleOrLong(in);
        return BluefloodGaugeRollup.fromBasicRollup(basic, timestamp, lastValue);
    }

}
