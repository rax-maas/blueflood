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

package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_FULL_RES;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_ROLLUP;

public class NumericSerializer extends AbstractSerializer<Object> {
    // NumericSerializer can be used with Rollup and full resolution metrics.

    private static NumericSerializer fullInstance = new NumericSerializer(true);
    private static NumericSerializer rollupInstance = new NumericSerializer(false);
    private boolean fullResolution;
    private static Histogram fullResSize = Metrics.newHistogram(NumericSerializer.class, "Full Resolution Metric Size");
    private static Histogram rollupSize = Metrics.newHistogram(NumericSerializer.class, "Rollup Metric Size");

    static class Type {
        static final byte B_ROLLUP_V1 = (byte)'r';
        static final byte B_FLOAT_AS_DOUBLE = (byte)'f';
        static final byte B_ROLLUP_STAT = (byte)'t';
    }

    private NumericSerializer(Boolean fullResolution) {
        this.fullResolution = fullResolution;
    }

    public static NumericSerializer get(ColumnFamily<Locator, Long> columnFamily) {
        if (columnFamily == null) {
            throw new RuntimeException("ColumnFamily cannot be null",
                    new SerializationException("ColumnFamily cannot be null"));
        }
        if (columnFamily.equals(AstyanaxIO.CF_METRICS_FULL)) {
            return fullInstance;
        }
        return rollupInstance;
    }

    @Override
    public ByteBuffer toByteBuffer(Object o) {
        try {
            byte type = typeOf(o);
            byte[] buf = new byte[sizeOf(o, type)];

            if (this.fullResolution) {
                serializeFullResMetric(o, buf);
            } else {  // dealing with rollup metrics
                if (o instanceof BasicRollup) {
                    BasicRollup basicRollup = (BasicRollup)o;
                    serializeRollup(basicRollup, buf);
                } else {
                    throw new SerializationException(String.format("Unexpected data type: %s", o.getClass().getName()));
                }
            }
            ByteBuffer out = ByteBuffer.wrap(buf);
            return out;

        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void serializeRollup(BasicRollup basicRollup, byte[] buf) throws IOException {
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        rollupSize.update(sizeOf(basicRollup, Type.B_ROLLUP_V1));
        protobufOut.writeRawByte(Constants.VERSION_1_ROLLUP);
        protobufOut.writeRawVarint64(basicRollup.getCount());          // stat count

        if (basicRollup.getCount() > 0) {
            putRollupStat(basicRollup.getAverage(), protobufOut);
            putRollupStat(basicRollup.getVariance(), protobufOut);
            putRollupStat(basicRollup.getMinValue(), protobufOut);
            putRollupStat(basicRollup.getMaxValue(), protobufOut);
        }
    }

    private void putRollupStat(AbstractRollupStat stat, CodedOutputStream protobufOut) throws IOException {
        protobufOut.writeRawByte(stat.getStatType());   // stat type
        protobufOut.writeRawByte(stat.isFloatingPoint() ? Constants.B_DOUBLE : Constants.B_I64);

        if (stat.isFloatingPoint()) {
            protobufOut.writeDoubleNoTag(stat.toDouble());
        } else {
            protobufOut.writeRawVarint64(stat.toLong());
        }
    }

    private void serializeFullResMetric(Object o, byte[] buf) throws IOException {
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        byte type = typeOf(o);
        fullResSize.update(sizeOf(o, type));
        protobufOut.writeRawByte(Constants.VERSION_1_FULL_RES);

        switch (type) {
            case Constants.B_I32:
                protobufOut.writeRawByte(type);
                protobufOut.writeRawVarint32((Integer) o);
                break;
            case Constants.B_I64:
                protobufOut.writeRawByte(type);
                protobufOut.writeRawVarint64((Long) o);
                break;
            case Constants.B_DOUBLE:
                protobufOut.writeRawByte(type);
                protobufOut.writeDoubleNoTag((Double) o);
                break;
            case Type.B_FLOAT_AS_DOUBLE:
                protobufOut.writeRawByte(Constants.B_DOUBLE);
                protobufOut.writeDoubleNoTag(((Float) o).doubleValue());
                break;
            default:
                throw new SerializationException(String.format("Cannot serialize %s", o.getClass().getName()));
        }
    }

    private static int sizeOf(Object o, byte type) throws IOException {
        int sz = 0;
        switch (type) {
            case Constants.B_I32:
                sz += 1 + 1; // version + type.
                sz += CodedOutputStream.computeRawVarint32Size((Integer)o);
                break;
            case Constants.B_I64:
                sz += 1 + 1; // version + type.
                sz += CodedOutputStream.computeRawVarint64Size((Long)o);
                break;
            case Constants.B_DOUBLE:
                sz += 1 + 1; // version + type.
                sz += CodedOutputStream.computeDoubleSizeNoTag((Double)o);
                break;
            case Type.B_FLOAT_AS_DOUBLE:
                sz += 1 + 1; // version + type.
                sz += CodedOutputStream.computeDoubleSizeNoTag(((Float)o).doubleValue());
                break;
            case Type.B_ROLLUP_V1:
                sz += 1; // version
                BasicRollup basicRollup = (BasicRollup)o;
                sz += CodedOutputStream.computeRawVarint64Size(basicRollup.getCount());
                if (basicRollup.getCount() > 0) {
                    sz += sizeOf(basicRollup.getAverage(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(basicRollup.getVariance(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(basicRollup.getMinValue(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(basicRollup.getMaxValue(), Type.B_ROLLUP_STAT);
                }
                break;
            case Type.B_ROLLUP_STAT:
                sz = 1 + 1; // type + isFP.
                AbstractRollupStat stat = (AbstractRollupStat)o;
                sz += stat.isFloatingPoint() ?
                        CodedOutputStream.computeDoubleSizeNoTag(stat.toDouble()) :
                        CodedOutputStream.computeRawVarint64Size(stat.toLong());
                return sz;
            default:
                throw new IOException("Unexpected type: " + type);
        }
        return sz;
    }


    private static byte typeOf(Object o) throws IOException {
        if (o instanceof Integer)
            return Constants.B_I32;
        else if (o instanceof Long)
            return Constants.B_I64;
        else if (o instanceof Double)
            return Constants.B_DOUBLE;
        else if (o instanceof Float)
            return Type.B_FLOAT_AS_DOUBLE;
        else if (o instanceof AbstractRollupStat)
            return Type.B_ROLLUP_STAT;
        else if (o instanceof BasicRollup)
            return Type.B_ROLLUP_V1;
        else
            throw new SerializationException("Unexpected type: " + o.getClass().getName());
    }


    @Override
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            if (version != VERSION_1_FULL_RES && version != VERSION_1_ROLLUP) {
                throw new SerializationException(String.format("Unexpected serialization version: %d",
                        (int)version));
            }
            if (this.fullResolution) {
                return deserializeSimpleMetric(in);
            } else {
                return deserializeV1Rollup(in);
            }
        } catch (Exception e) {
            throw new RuntimeException("Deserialization Failure", e);
        }
    }

    private Object deserializeV1Rollup(CodedInputStream in) throws IOException {
        final BasicRollup basicRollup = new BasicRollup();
        final long count = in.readRawVarint64();
        basicRollup.setCount(count);

        if (count <= 0) {
            return basicRollup;
        }

        while (!in.isAtEnd()) {
            byte statType = in.readRawByte();
            AbstractRollupStat stat = getStatFromRollup(statType, basicRollup);

            if (stat == null) {
                throw new IOException("V1 BasicRollup: Unable to determine stat of type " + (int)statType);
            }

            byte metricValueType = in.readRawByte();

            switch (metricValueType) {
                case Constants.I64:
                    stat.setLongValue(in.readRawVarint64());
                    break;
                case Constants.B_DOUBLE:
                    stat.setDoubleValue(in.readDouble());
                    break;
                default:
                    throw new IOException("Unsupported metric value type " + (int)metricValueType);
            }
        }
        return basicRollup;
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
            default: throw new SerializationException(String.format("Unexpected raw metric type=%s for full res " +
                    "metric", (char)metricValueType));
        }
    }

    private AbstractRollupStat getStatFromRollup(byte statType, BasicRollup basicRollup) {
        switch (statType) {
            case Constants.AVERAGE:
                return basicRollup.getAverage();
            case Constants.VARIANCE:
                return basicRollup.getVariance();
            case Constants.MIN:
                return basicRollup.getMinValue();
            case Constants.MAX:
                return basicRollup.getMaxValue();
            default:
                return null;
        }
    }
}