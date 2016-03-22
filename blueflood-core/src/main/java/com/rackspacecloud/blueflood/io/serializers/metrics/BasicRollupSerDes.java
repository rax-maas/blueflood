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
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_FULL_RES;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;

/**
 * This class knows how to serialize/deserialize a BasicRollup to its byte
 * wire format.
 */
public class BasicRollupSerDes extends AbstractSerDes {

    /**
     * Our internal metric to count the number of rollups
     */
    protected static Histogram rollupSize = Metrics.histogram(BasicRollupSerDes.class, "Rollup Metric Size");

    public ByteBuffer serialize(BasicRollup basicRollup) {
        try {
            byte[] buf = new byte[sizeOf(basicRollup)];
            serializeRollup(basicRollup, buf);
            return ByteBuffer.wrap(buf);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BasicRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_FULL_RES && version != VERSION_1_ROLLUP) {
                throw new SerializationException(String.format("Unexpected serialization version: %d",
                        (int)version));
            }
            return deserializeV1Rollup(in);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization Failure", e);
        }
    }

    protected BasicRollup deserializeV1Rollup(CodedInputStream in) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        final long count = in.readRawVarint64();
        basicRollup.setCount(count);

        if (count <= 0) {
            return basicRollup;
        }

        for (int i = 0; i < BasicRollup.NUM_STATS; i++) {
            byte statType = in.readRawByte();
            switch (statType) {
                case Constants.AVERAGE:
                    averageStatDeSer.deserialize(basicRollup.getAverage(), in);
                    break;
                case Constants.VARIANCE:
                    varianceStatDeSer.deserialize(basicRollup.getVariance(), in);
                    break;
                case Constants.MIN:
                    minStatDeSer.deserialize(basicRollup.getMinValue(), in);
                    break;
                case Constants.MAX:
                    maxStatDeSer.deserialize(basicRollup.getMaxValue(), in);
                    break;
                default:
                    throw new SerializationException("invalid stat v1 type: " + (int) statType);
            }
        }
        return basicRollup;
    }

    protected int sizeOf(BasicRollup basicRollup) {
        int sz = sizeOfSize();
        sz += CodedOutputStream.computeRawVarint64Size(basicRollup.getCount());
        if (basicRollup.getCount() > 0) {
            sz += averageStatDeSer.sizeOf(basicRollup.getAverage());
            sz += varianceStatDeSer.sizeOf(basicRollup.getVariance());
            sz += minStatDeSer.sizeOf(basicRollup.getMinValue());
            sz += maxStatDeSer.sizeOf(basicRollup.getMaxValue());
        }
        return sz;
    }

    protected void serializeRollup(BasicRollup basicRollup, byte[] buf) throws IOException {
        rollupSize.update(buf.length);
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        serializeRollup(basicRollup, protobufOut);
    }

    protected void serializeRollup(BasicRollup basicRollup, CodedOutputStream protobufOut) throws IOException {
        protobufOut.writeRawByte(Constants.VERSION_1_ROLLUP);
        protobufOut.writeRawVarint64(basicRollup.getCount());          // stat count

        if (basicRollup.getCount() > 0) {
            putRollupStat(basicRollup.getAverage(), protobufOut);
            putRollupStat(basicRollup.getVariance(), protobufOut);
            putRollupStat(basicRollup.getMinValue(), protobufOut);
            putRollupStat(basicRollup.getMaxValue(), protobufOut);
        }
    }
}
