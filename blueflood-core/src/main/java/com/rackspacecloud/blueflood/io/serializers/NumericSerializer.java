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

package com.rackspacecloud.blueflood.io.serializers;

import com.codahale.metrics.Histogram;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.rackspacecloud.blueflood.io.Constants.*;

public class NumericSerializer {
    // NumericSerializer can be used with Rollup and full resolution metrics.
    
    private static final boolean DUMP_BAD_BUFFERS = System.getProperty("DUMP_BAD_BUFFERS") != null;

    public static final AbstractSerializer<SimpleNumber> simpleNumberSerializer = new SimpleNumberSerializer();
    private static AbstractSerializer<Object> fullInstance = new RawSerializer();
    private static AbstractSerializer<BasicRollup> basicRollupInstance = new BasicRollupSerializer();
    public static AbstractSerializer<BluefloodTimerRollup> timerRollupInstance = new TimerRollupSerializer();
    public static AbstractSerializer<BluefloodSetRollup> setRollupInstance = new SetRollupSerializer();
    public static AbstractSerializer<BluefloodGaugeRollup> gaugeRollupInstance = new GaugeRollupSerializer();
    public static AbstractSerializer<BluefloodCounterRollup> CounterRollupInstance = new CounterRollupSerializer();
    
    private static Histogram fullResSize = Metrics.histogram(NumericSerializer.class, "Full Resolution Metric Size");
    private static Histogram rollupSize = Metrics.histogram(NumericSerializer.class, "Rollup Metric Size");
    private static Histogram CounterRollupSize = Metrics.histogram(NumericSerializer.class, "Counter Gauge Metric Size");
    private static Histogram SetRollupSize = Metrics.histogram(NumericSerializer.class, "Set Metric Size");
    private static Histogram timerRollupSize = Metrics.histogram(NumericSerializer.class, "Timer Metric Size");

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
        else if (type.equals(BluefloodTimerRollup.class))
            return (AbstractSerializer<T>)timerRollupInstance;
        else if (type.equals(HistogramRollup.class))
            return (AbstractSerializer<T>) HistogramSerializer.get();
        else if (type.equals(BluefloodCounterRollup.class))
            return (AbstractSerializer<T>) CounterRollupInstance;
        else if (type.equals(BluefloodGaugeRollup.class))
            return (AbstractSerializer<T>)gaugeRollupInstance;
        else if (type.equals(BluefloodSetRollup.class))
            return (AbstractSerializer<T>)setRollupInstance;
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
        rollupSize.update(buf.length);
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
    
    // put out a number prefaced only by a type.
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

    private static int sizeOf(Object o, byte type) 
      throws IOException {
        return sizeOf(o, type, VERSION_2_TIMER);
    }

    private static int sizeOf(Object o, byte type, byte timerVersion) 
      throws IOException {
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
            case Type.B_SET:
                sz += 1; // version
                BluefloodSetRollup setRollup = (BluefloodSetRollup)o;
                sz += CodedOutputStream.computeRawVarint32Size(setRollup.getCount());
                for (Integer i : setRollup.getHashes()) {
                    sz += CodedOutputStream.computeRawVarint32Size(i);
                }
                break;
            case Type.B_ROLLUP_STAT:
                sz = 1 + 1; // type + isFP.
                AbstractRollupStat stat = (AbstractRollupStat)o;
                sz += stat.isFloatingPoint() ?
                        CodedOutputStream.computeDoubleSizeNoTag(stat.toDouble()) :
                        CodedOutputStream.computeRawVarint64Size(stat.toLong());
                return sz;
            case Type.B_TIMER:
                sz += 1; // version
                BluefloodTimerRollup rollup = (BluefloodTimerRollup)o;
                if (timerVersion == VERSION_1_TIMER) {
                    sz += CodedOutputStream.computeRawVarint64Size((long) rollup.getSum());
                } else if (timerVersion == VERSION_2_TIMER) {

                    sz += CodedOutputStream.computeDoubleSizeNoTag(rollup.getSum());
                } else {
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)timerVersion));                    
                }
                sz += CodedOutputStream.computeRawVarint64Size(rollup.getCount());
                sz += CodedOutputStream.computeDoubleSizeNoTag(rollup.getRate());
                sz += CodedOutputStream.computeRawVarint32Size(rollup.getSampleCount());
                sz += sizeOf(rollup.getAverage(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getMaxValue(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getMinValue(), Type.B_ROLLUP_STAT);
                sz += sizeOf(rollup.getVariance(), Type.B_ROLLUP_STAT);
                
                Map<String, BluefloodTimerRollup.Percentile> percentiles = rollup.getPercentiles();
                sz += CodedOutputStream.computeRawVarint32Size(rollup.getPercentiles().size());
                for (Map.Entry<String, BluefloodTimerRollup.Percentile> entry : percentiles.entrySet()) {
                    sz += CodedOutputStream.computeStringSizeNoTag(entry.getKey());
                    Number[] pctComponents = new Number[] {
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
                // just like rollup up until a point.
                sz += sizeOf(o, Type.B_ROLLUP);
                
                // here's where it gets different.
                BluefloodGaugeRollup gauge = (BluefloodGaugeRollup)o;
                sz += CodedOutputStream.computeRawVarint64Size(gauge.getTimestamp());
                sz += 1; // type of latest value.
                if (gauge.getLatestNumericValue() instanceof Long || gauge.getLatestNumericValue() instanceof Integer)
                    sz += CodedOutputStream.computeRawVarint64Size(gauge.getLatestNumericValue().longValue());
                else if (gauge.getLatestNumericValue() instanceof Double || gauge.getLatestNumericValue() instanceof Float)
                    sz += CodedOutputStream.computeDoubleSizeNoTag(gauge.getLatestNumericValue().doubleValue());
                return sz;
                
            case Type.B_COUNTER:
                BluefloodCounterRollup counter = (BluefloodCounterRollup)o;
                sz += 1; // version + rollup type.
                sz += 1; // numeric type.
                if (counter.getCount() instanceof Long || counter.getCount() instanceof Integer)
                    sz += CodedOutputStream.computeRawVarint64Size(counter.getCount().longValue());
                else if (counter.getCount() instanceof Double || counter.getCount() instanceof Float)
                    sz += CodedOutputStream.computeDoubleSizeNoTag(counter.getCount().doubleValue());
                sz += CodedOutputStream.computeDoubleSizeNoTag(counter.getRate());
                sz += CodedOutputStream.computeRawVarint32Size(counter.getSampleCount());
                return sz;
            default:
                throw new IOException("Unexpected type: " + type);
        }
        return sz;
    }
    
    private static void serializeCounterRollup(BluefloodCounterRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        CounterRollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_COUNTER_ROLLUP);
        putUnversionedDoubleOrLong(rollup.getCount(), out);
        out.writeDoubleNoTag(rollup.getRate());
        out.writeRawVarint32(rollup.getSampleCount());
    }
    
    private static BluefloodCounterRollup deserializeV1CounterRollup(CodedInputStream in) throws IOException {
        Number value = getUnversionedDoubleOrLong(in);
        double rate = in.readDouble();
        int sampleCount = in.readRawVarint32();
        return new BluefloodCounterRollup().withCount(value.longValue()).withRate(rate).withSampleCount(sampleCount);
    }
    
    private static void serializeSetRollup(BluefloodSetRollup rollup, byte[] buf) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        SetRollupSize.update(buf.length);
        out.writeRawByte(Constants.VERSION_1_SET_ROLLUP);
        out.writeRawVarint32(rollup.getCount());
        for (Integer i : rollup.getHashes()) {
            out.writeRawVarint32(i);
        }
    }
    
    private static BluefloodSetRollup deserializeV1SetRollup(CodedInputStream in) throws IOException {
        int count = in.readRawVarint32();
        BluefloodSetRollup rollup = new BluefloodSetRollup();
        while (count-- > 0) {
            rollup = rollup.withObject(in.readRawVarint32());
        }
        return rollup;
    }

    private static void serializeTimer(BluefloodTimerRollup rollup, byte[] buf, byte timerVersion) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        timerRollupSize.update(buf.length);
        out.writeRawByte(timerVersion);
        
        // sum, count, countps, avg, max, min, var
        if (timerVersion == VERSION_1_TIMER) {
            out.writeRawVarint64((long)rollup.getSum());
        } else if (timerVersion == VERSION_2_TIMER) {
            out.writeDoubleNoTag(rollup.getSum());
        } else {
            throw new SerializationException(String.format("Unexpected serialization version: %d", (int)timerVersion));                    
        }

        out.writeRawVarint64(rollup.getCount());
        out.writeDoubleNoTag(rollup.getRate());
        out.writeRawVarint32(rollup.getSampleCount());
        putRollupStat(rollup.getAverage(), out);
        putRollupStat(rollup.getMaxValue(), out);
        putRollupStat(rollup.getMinValue(), out);
        putRollupStat(rollup.getVariance(), out);
        
        // percentiles.
        Map<String, BluefloodTimerRollup.Percentile> percentiles = rollup.getPercentiles();
        out.writeRawVarint32(percentiles.size());
        for (Map.Entry<String, BluefloodTimerRollup.Percentile> entry : percentiles.entrySet()) {
            out.writeStringNoTag(entry.getKey());
            putUnversionedDoubleOrLong(entry.getValue().getMean(), out);
        }
    }
    
    private static BluefloodTimerRollup deserializeTimer(CodedInputStream in, byte timerVersion) throws IOException {
        // note: type and version have already been read.
        final double sum;
        if (timerVersion == VERSION_1_TIMER) {
             sum = in.readRawVarint64();
        } else if (timerVersion == VERSION_2_TIMER) {
             sum = in.readDouble();
        } else {
            throw new SerializationException(String.format("Unexpected serialization version: %d", (int)timerVersion));                    
        }


        final long count = in.readRawVarint64();
        final double countPs = in.readDouble();
        final int sampleCount = in.readRawVarint32();
        
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
        
        BluefloodTimerRollup rollup = new BluefloodTimerRollup()
                .withSum(sum)
                .withCount(count)
                .withCountPS(countPs)
                .withSampleCount(sampleCount)
                .withAverage(statBucket.getAverage())
                .withMaxValue(statBucket.getMaxValue())
                .withMinValue(statBucket.getMinValue())
                .withVariance(statBucket.getVariance());
        
        int numPercentiles = in.readRawVarint32();
        for (int i = 0; i < numPercentiles; i++) {
            String name = in.readString();
            Number mean = getUnversionedDoubleOrLong(in);
            rollup.setPercentile(name, mean);
        }
        
        return rollup;
    }
    
    private static void serializeGauge(BluefloodGaugeRollup rollup, byte[] buf) throws IOException {
        rollupSize.update(buf.length);
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);
        serializeRollup(rollup, protobufOut);
        protobufOut.writeRawVarint64(rollup.getTimestamp());
        putUnversionedDoubleOrLong(rollup.getLatestNumericValue(), protobufOut);
    }
    
    private static BluefloodGaugeRollup deserializeV1Gauge(CodedInputStream in) throws IOException {
        BasicRollup basic = deserializeV1Rollup(in);
        long timestamp = in.readRawVarint64();
        Number lastValue = getUnversionedDoubleOrLong(in);
        return BluefloodGaugeRollup.fromBasicRollup(basic, timestamp, lastValue);
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
        else if (o instanceof BluefloodTimerRollup)
            return Type.B_TIMER;
        else if (o instanceof BluefloodGaugeRollup)
            return Type.B_GAUGE;
        else if (o instanceof BluefloodSetRollup)
            return Type.B_SET;
        else if (o instanceof BasicRollup)
            return Type.B_ROLLUP;
        else if (o instanceof BluefloodCounterRollup)
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

        for (int i = 0; i < BasicRollup.NUM_STATS; i++) {
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
    private static void dumpBufferUnsafe(CodedInputStream in) {
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
    
    public static class RawSerializer extends AbstractSerializer<Object> {
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
    
    public static class TimerRollupSerializer extends AbstractSerializer<BluefloodTimerRollup> {
        @Override
        public ByteBuffer toByteBuffer(BluefloodTimerRollup o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type, VERSION_2_TIMER)];
                serializeTimer(o, buf, VERSION_2_TIMER);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @VisibleForTesting
        public ByteBuffer toByteBufferWithV1Serialization(BluefloodTimerRollup o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type, VERSION_1_TIMER)];
                serializeTimer(o, buf, VERSION_1_TIMER);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public BluefloodTimerRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                return deserializeTimer(in, version);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    public static class SetRollupSerializer extends AbstractSerializer<BluefloodSetRollup> {
        
        @Override
        public ByteBuffer toByteBuffer(BluefloodSetRollup obj) {
            try {
                byte type = typeOf(obj);
                byte[] buf = new byte[sizeOf(obj, type)];
                serializeSetRollup(obj, buf);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public BluefloodSetRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                if (version != VERSION_1_SET_ROLLUP)
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)version));
                return deserializeV1SetRollup(in);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    public static class GaugeRollupSerializer extends AbstractSerializer<BluefloodGaugeRollup> {
        @Override
        public ByteBuffer toByteBuffer(BluefloodGaugeRollup o) {
            try {
                byte type = typeOf(o);
                byte[] buf = new byte[sizeOf(o, type)];
                serializeGauge(o, buf);
                return ByteBuffer.wrap(buf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public BluefloodGaugeRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                if (version != VERSION_1_ROLLUP)
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)version));
                return deserializeV1Gauge(in);
            } catch (Exception e) {
                throw new RuntimeException("Deserialization Failure", e);
            }
        }
    }
    
    // for now let's try to get away with a single serializer for all single value rollups. We'll still encode specific
    // types so we can differentiate.
    public static class CounterRollupSerializer extends AbstractSerializer<BluefloodCounterRollup> {
        @Override
        public ByteBuffer toByteBuffer(BluefloodCounterRollup obj) {
            try {
                byte type = typeOf(obj);
                byte[] buf = new byte[sizeOf(obj, type)];
                serializeCounterRollup(obj, buf);
                return ByteBuffer.wrap(buf);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public BluefloodCounterRollup fromByteBuffer(ByteBuffer byteBuffer) {
            CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
            try {
                byte version = in.readRawByte();
                if (version != VERSION_1_COUNTER_ROLLUP)
                    throw new SerializationException(String.format("Unexpected serialization version: %d", (int)version));
                return deserializeV1CounterRollup(in);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
