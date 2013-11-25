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

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.HistogramRollup;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.SingleValueRollup;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.rackspacecloud.blueflood.io.Constants.*;

public class NumericSerializer {
    // NumericSerializer can be used with Rollup and full resolution metrics.
    
    private static final boolean DUMP_BAD_BUFFERS = System.getProperty("DUMP_BAD_BUFFERS") != null;

    public static final AbstractSerializer<SimpleNumber> simpleNumberSerializer = new SimpleNumberSerializer();
    private static AbstractSerializer<Object> fullInstance = new RawSerializer();
    private static AbstractSerializer<BasicRollup> basicRollupInstance = new BasicRollupSerializer();
    private static AbstractSerializer<TimerRollup> timerRollupInstance = new TimerRollupSerializer();
    private static AbstractSerializer<SingleValueRollup> singleValueRollup = new SingleValueRollupSerializer();
    
    private static Histogram fullResSize = Metrics.newHistogram(NumericSerializer.class, "Full Resolution Metric Size");
    private static Histogram rollupSize = Metrics.newHistogram(NumericSerializer.class, "Rollup Metric Size");

    static class Type {
        static final byte B_ROLLUP = (byte)'r';
        static final byte B_FLOAT_AS_DOUBLE = (byte)'f';
        static final byte B_ROLLUP_STAT = (byte)'t';
        static final byte B_COUNTER = (byte)'C';
        static final byte B_TIMER = (byte)'T';
        static final byte B_SET = (byte)'S';
        static final byte B_GAUGE = (byte)'G';
    }
    
    /** return a serializer for a specific type */
    public static <T> AbstractSerializer<T> serializerFor(Class<T> type) {
        if (type == null)
            throw new RuntimeException("serializable type cannot be null",
                    new SerializationException("serializable type cannot be null"));
        else if (type.equals(String.class))
            throw new RuntimeException("We don't serialize strings anymore",
                    new SerializationException("We don't serialize strings anymore"));
        
        if (type.equals(BasicRollup.class))
            return (AbstractSerializer<T>) basicRollupInstance;
        else if (type.equals(TimerRollup.class))
            return (AbstractSerializer<T>)timerRollupInstance;
        else if (type.equals(HistogramRollup.class))
            throw new RuntimeException("Not implemented yet");
        else if (type.equals(CounterRollup.class))
            return (AbstractSerializer<T>)singleValueRollup;
        else if (type.equals(GaugeRollup.class))
            return (AbstractSerializer<T>)singleValueRollup;
        else if (type.equals(SetRollup.class))
            return (AbstractSerializer<T>)singleValueRollup;
        else if (type.equals(SimpleNumber.class))
            return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(Integer.class))
            return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(Long.class))
            return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(Double.class))
            return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(Float.class))
                return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(byte[].class))
            return (AbstractSerializer<T>)fullInstance;
        else if (type.equals(Object.class))
            return (AbstractSerializer<T>)fullInstance;
        else
            return (AbstractSerializer<T>)fullInstance;   
    }

    private static void serializeRollup(BasicRollup basicRollup, byte[] buf) throws IOException {
        // todo: simplify: rollupSize.update(buf.length); right?
        rollupSize.update(sizeOf(basicRollup, Type.B_ROLLUP));
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        serializeRollup(basicRollup, protobufOut);
    }
    
    private static void serializeRollup(BasicRollup basicRollup, CodedOutputStream protobufOut) throws IOException {
        protobufOut.writeRawByte(Constants.VERSION_1_ROLLUP);
        protobufOut.writeRawVarint64(basicRollup.getCount());          // stat count

        if (basicRollup.getCount() > 0) {
            putRollupStat(basicRollup.getAverage(), protobufOut);
            putRollupStat(basicRollup.getVariance(), protobufOut);
            putRollupStat(basicRollup.getMinValue(), protobufOut);
            putRollupStat(basicRollup.getMaxValue(), protobufOut);
        }
    }

    private static void putRollupStat(AbstractRollupStat stat, CodedOutputStream protobufOut) throws IOException {
        protobufOut.writeRawByte(stat.getStatType());   // stat type
        protobufOut.writeRawByte(stat.isFloatingPoint() ? Constants.B_DOUBLE : Constants.B_I64);

        if (stat.isFloatingPoint()) {
            protobufOut.writeDoubleNoTag(stat.toDouble());
        } else {
            protobufOut.writeRawVarint64(stat.toLong());
        }
    }
    
    // write out a number prefaced only by a type.
    private static void putUnversionedDoubleOrLong(Number number, CodedOutputStream out) throws IOException {
        if (number instanceof Double) {
            out.writeRawByte(Constants.B_DOUBLE);
            out.writeDoubleNoTag(number.doubleValue());
        } else {
            out.writeRawByte(Constants.B_I64);
            out.writeRawVarint64(number.longValue());
        }
    }
    
    // read out a type-specified number.
    public static Number getUnversionedDoubleOrLong(CodedInputStream in) throws IOException {
        byte type = in.readRawByte();
        if (type == Constants.B_DOUBLE)
            return in.readDouble();
        else
            return in.readRawVarint64();
    }

    private static void serializeFullResMetric(Object o, byte[] buf) throws IOException {
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
            case Type.B_ROLLUP:
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
            case Type.B_TIMER: // TimerRollup
                sz += 1; // version
                TimerRollup rollup = (TimerRollup)o;
                sz += CodedOutputStream.computeRawVarint64Size(rollup.getSum());
                sz += CodedOutputStream.computeRawVarint64Size(rollup.getCount());
                sz += CodedOutputStream.computeDoubleSizeNoTag(rollup.getCountPS());
                sz += sizeOf(rollup.getAverage(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getMaxValue(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getMinValue(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getVariance(), Type.B_ROLLUP_STAT);
                
                Map<String, TimerRollup.Percentile> percentiles = rollup.getPercentiles();
                sz += CodedOutputStream.computeRawVarint32Size(rollup.getPercentiles().size());
                for (Map.Entry<String, TimerRollup.Percentile> entry : percentiles.entrySet()) {
                    sz += CodedOutputStream.computeStringSizeNoTag(entry.getKey());
                    Number[] pctComponents = new Number[] {
                            entry.getValue().getSum(),
                            entry.getValue().getUpper(),
                            entry.getValue().getMean(),
                    };
                    for (Number num : pctComponents) {
                        sz += 1; // type.
                        if (num instanceof Long || num instanceof Integer) {
                            sz += CodedOutputStream.computeRawVarint64Size(num.longValue());
                        } else if (num instanceof Double || num instanceof Float) {
                            sz += CodedOutputStream.computeDoubleSizeNoTag(num.doubleValue());
                        }
                    }
                }
                return sz;
                
            case Type.B_GAUGE:
            case Type.B_SET:
            case Type.B_COUNTER:
                SingleValueRollup svr = (SingleValueRollup)o;
                sz += 2; // version + rollup type.
                sz += 1; // numeric type.
                if (svr.getValue() instanceof Long || svr.getValue() instanceof Integer)
                    sz += CodedOutputStream.computeRawVarint64Size(svr.getValue().longValue());
                else if (svr.getValue() instanceof Double || svr.getValue() instanceof Float)
                    sz += CodedOutputStream.computeDoubleSizeNoTag(svr.getValue().doubleValue());
                return sz;
            default:
                throw new IOException("Unexpected type: " + type);
        }
        return sz;
    }
    
    private static void serializeSingleValueRollup(SingleValueRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        out.writeRawByte(Constants.VERSION_1_SINGLE_VALUE_ROLLUP);
        out.writeRawByte(typeOf(rollup));
        putUnversionedDoubleOrLong(rollup.getValue(), out);
    }
    
    private static SingleValueRollup deserializeV1SingleValueRollup(CodedInputStream in) throws IOException {
        byte type = in.readRawByte();
        Number value = getUnversionedDoubleOrLong(in);
        switch (type) {
            case Type.B_COUNTER:
                return new CounterRollup(1).withCount(value.longValue());
            case Type.B_SET:
                return new SetRollup().withCount(value.longValue());
            case Type.B_GAUGE:
                return new GaugeRollup().withGauge(value);
            default:
                throw new IOException("Unexpected single value rollup type: " + new BigInteger(new byte[]{type}).toString(16));
        }
    }
    
    private static void serializeTimer(TimerRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        rollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_TIMER);
        
        // sum, count, countps, avg, max, min, var
        out.writeRawVarint64(rollup.getSum());
        out.writeRawVarint64(rollup.getCount());
        out.writeDoubleNoTag(rollup.getCountPS());
        putRollupStat(rollup.getAverage(), out);
        putRollupStat(rollup.getMaxValue(), out);
        putRollupStat(rollup.getMinValue(), out);
        putRollupStat(rollup.getVariance(), out);
        
        // percentiles.
        Map<String, TimerRollup.Percentile> percentiles = rollup.getPercentiles();
        out.writeRawVarint32(percentiles.size());
        for (Map.Entry<String, TimerRollup.Percentile> entry : percentiles.entrySet()) {
            out.writeStringNoTag(entry.getKey());
            putUnversionedDoubleOrLong(entry.getValue().getMean(), out);
            putUnversionedDoubleOrLong(entry.getValue().getUpper(), out);
            putUnversionedDoubleOrLong(entry.getValue().getSum(), out);
        }
    }
    
    private static TimerRollup deserializeV1Timer(CodedInputStream in) throws IOException {
        // note: type and version have already been read.
        final long sum = in.readRawVarint64();
        final long count = in.readRawVarint64();
        final double countPs = in.readDouble();
        
        BasicRollup statBucket = new BasicRollup();
        
        byte statType;
        AbstractRollupStat stat;
        
        // average
        statType = in.readRawByte();
        stat = getStatFromRollup(statType, statBucket);
        setStat(stat, in);
        // max
        statType = in.readRawByte();
        stat = getStatFromRollup(statType, statBucket);
        setStat(stat, in);
        // min
        statType = in.readRawByte();
        stat = getStatFromRollup(statType, statBucket);
        setStat(stat, in);
        // var
        statType = in.readRawByte();
        stat = getStatFromRollup(statType, statBucket);
        setStat(stat, in);
        
        TimerRollup rollup = new TimerRollup()
                .withSum(sum)
                .withCount(count)
                .withCountPS(countPs)
                .withAverage(statBucket.getAverage())
                .withMaxValue(statBucket.getMaxValue())
                .withMinValue(statBucket.getMinValue())
                .withVariance(statBucket.getVariance());
        
        int numPercentiles = in.readRawVarint32();
        for (int i = 0; i < numPercentiles; i++) {
            String name = in.readString();
            Number mean = getUnversionedDoubleOrLong(in);
            Number upper = getUnversionedDoubleOrLong(in);
            Number pctSum = getUnversionedDoubleOrLong(in);
            rollup.setPercentile(name, mean, pctSum, upper);
        }
        
        return rollup;
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
        else if (o instanceof TimerRollup)
            return Type.B_TIMER;
        else if (o instanceof BasicRollup)
            return Type.B_ROLLUP;
        else if (o instanceof GaugeRollup)
            return Type.B_GAUGE;
        else if (o instanceof SetRollup)
            return Type.B_SET;
        else if (o instanceof CounterRollup)
            return Type.B_COUNTER;
        else
            throw new SerializationException("Unexpected type: " + o.getClass().getName());
    }

    private static BasicRollup deserializeV1Rollup(CodedInputStream in) throws IOException {
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
            setStat(stat, in);
        }
        return basicRollup;
    }
    
    // todo: this should return an instance instead of populate one, but will require some refatoring of 
    // deserializeV1Rollup().
    private static void setStat(AbstractRollupStat stat, CodedInputStream in) throws IOException {
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
    
    private static Object deserializeSimpleMetric(CodedInputStream in) throws IOException {
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
    
    // handy utility to dump bad buffers when they are encountered. e.g. during serialization debugging.
    private static void dumpBuffer(CodedInputStream in) {
        if (DUMP_BAD_BUFFERS) {
            try {
                Field bufferField = in.getClass().getDeclaredField("buffer");
                bufferField.setAccessible(true);
                byte[] buffer = (byte[])bufferField.get(in);
                OutputStream out = new FileOutputStream(File.createTempFile(String.format("bf_bad_buffer_%d_%d", System.currentTimeMillis(), System.nanoTime()), ".bin"));
                out.write(buffer);
                out.close();
            } catch (Throwable th) {
                // ignore.
            }
        }
    }

    private static AbstractRollupStat getStatFromRollup(byte statType, BasicRollup basicRollup) {
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
    
    private static class RawSerializer extends AbstractSerializer<Object> {
        @Override
        public ByteBuffer toByteBuffer(Object o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type)];
    
                serializeFullResMetric(o, buf);
                
                ByteBuffer out = ByteBuffer.wrap(buf);
                return out;
    
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
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
                return deserializeSimpleMetric(in);
            } catch (Exception e) {
                throw new RuntimeException("Deserialization Failure", e);
            }
        }
    }
    
    // composes a raw serializer.
    private static class SimpleNumberSerializer extends AbstractSerializer<SimpleNumber> {
        private static final RawSerializer rawSerde = new RawSerializer();
        
        @Override
        public ByteBuffer toByteBuffer(SimpleNumber obj) {
            return rawSerde.toByteBuffer(obj);
        }

        @Override
        public SimpleNumber fromByteBuffer(ByteBuffer byteBuffer) {
            return new SimpleNumber(rawSerde.fromByteBuffer(byteBuffer));
        }
    }
    
    private static class BasicRollupSerializer extends AbstractSerializer<BasicRollup> {
        @Override
        public ByteBuffer toByteBuffer(BasicRollup o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type)];
                serializeRollup(o, buf);
                return ByteBuffer.wrap(buf);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public BasicRollup fromByteBuffer(ByteBuffer byteBuffer) {
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
    }
    
    public static class TimerRollupSerializer extends AbstractSerializer<TimerRollup> {
        @Override
        public ByteBuffer toByteBuffer(TimerRollup o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type)];
                serializeTimer(o, buf);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public TimerRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                if (version != VERSION_1_TIMER)
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)version));
                return deserializeV1Timer(in);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    // for now let's try to get away with a single serializer for all single value rollups. We'll still encode specific
    // types so we can differentiate.
    public static class SingleValueRollupSerializer extends AbstractSerializer<SingleValueRollup> {
        @Override
        public ByteBuffer toByteBuffer(SingleValueRollup obj) {
            try {
                byte type = typeOf(obj);
                byte[] buf = new byte[sizeOf(obj, type)];
                serializeSingleValueRollup(obj, buf);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public SingleValueRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                if (version != VERSION_1_SINGLE_VALUE_ROLLUP)
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)version));
                return deserializeV1SingleValueRollup(in);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}