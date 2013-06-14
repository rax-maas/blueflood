package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.exceptions.SerializationException;
import com.cloudkick.blueflood.exceptions.UnexpectedStringSerializationException;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.AbstractRollupStat;
import com.cloudkick.blueflood.types.Rollup;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.cloudkick.blueflood.io.Constants.VERSION_1_FULL_RES;
import static com.cloudkick.blueflood.io.Constants.VERSION_1_ROLLUP;

public class NumericSerializer extends AbstractSerializer<Object> {
    // NumericSerializer can be used with Rollup and full resolution metrics.

    private static NumericSerializer fullInstance = new NumericSerializer(true);
    private static NumericSerializer rollupInstance = new NumericSerializer(false);
    private boolean fullResolution;

    static class Type {
        static final byte B_ROLLUP_V1 = (byte)'r';
        static final byte B_FLOAT_AS_DOUBLE = (byte)'f';
        static final byte B_ROLLUP_STAT = (byte)'t';
    }

    private NumericSerializer(Boolean fullResolution) {
        this.fullResolution = fullResolution;
    }

    public static NumericSerializer get(Granularity granularity) {
        if (granularity == null) {
            throw new RuntimeException("Granularity cannot be null", new SerializationException("Granularity cannot be null"));
        }
        if (granularity.equals(Granularity.FULL)) {
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
                if (o instanceof Rollup) {
                    Rollup rollup = (Rollup)o;
                    serializeRollup(rollup, buf);
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

    private void serializeRollup(Rollup rollup, byte[] buf) throws IOException {
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        protobufOut.writeRawByte(Constants.VERSION_1_ROLLUP);
        protobufOut.writeRawVarint64(rollup.getCount());          // stat count

        if (rollup.getCount() > 0) {
            putRollupStat(rollup.getAverage(), protobufOut);
            putRollupStat(rollup.getVariance(), protobufOut);
            putRollupStat(rollup.getMinValue(), protobufOut);
            putRollupStat(rollup.getMaxValue(), protobufOut);
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
                Rollup rollup = (Rollup)o;
                sz += CodedOutputStream.computeRawVarint64Size(rollup.getCount());
                if (rollup.getCount() > 0) {
                    sz += sizeOf(rollup.getAverage(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(rollup.getVariance(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(rollup.getMinValue(), Type.B_ROLLUP_STAT);
                    sz += sizeOf(rollup.getMaxValue(), Type.B_ROLLUP_STAT);
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
        else if (o instanceof Rollup)
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
        final Rollup rollup = new Rollup();
        final long count = in.readRawVarint64();
        rollup.setCount(count);

        if (count <= 0) {
            return rollup;
        }

        while (!in.isAtEnd()) {
            byte statType = in.readRawByte();
            AbstractRollupStat stat = getStatFromRollup(statType, rollup);

            if (stat == null) {
                throw new IOException("V1 Rollup: Unable to determine stat of type " + (int)statType);
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
        return rollup;
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

    private AbstractRollupStat getStatFromRollup(byte statType, Rollup rollup) {
        switch (statType) {
            case Constants.AVERAGE:
                return rollup.getAverage();
            case Constants.VARIANCE:
                return rollup.getVariance();
            case Constants.MIN:
                return rollup.getMinValue();
            case Constants.MAX:
                return rollup.getMaxValue();
            default:
                return null;
        }
    }
}
