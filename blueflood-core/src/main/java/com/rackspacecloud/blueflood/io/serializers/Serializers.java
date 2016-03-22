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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.exceptions.UnexpectedStringSerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.serializers.astyanax.HistogramSerializer;
import com.rackspacecloud.blueflood.io.serializers.metrics.*;
import com.rackspacecloud.blueflood.types.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.rackspacecloud.blueflood.io.Constants.*;

public class Serializers {
    // NumericSerializer can be used with Rollup and full resolution metrics.

    public static final AbstractSerializer<SimpleNumber> simpleNumberSerializer = new SimpleNumberSerializer();
    private static AbstractSerializer<Object> fullInstance = new RawSerializer();
    private static AbstractSerializer<BasicRollup> basicRollupInstance = new BasicRollupSerializer();
    public static AbstractSerializer<BluefloodTimerRollup> timerRollupInstance = new TimerRollupSerializer();
    public static AbstractSerializer<BluefloodSetRollup> setRollupInstance = new SetRollupSerializer();
    public static AbstractSerializer<BluefloodGaugeRollup> gaugeRollupInstance = new GaugeRollupSerializer();
    public static AbstractSerializer<BluefloodCounterRollup> CounterRollupInstance = new CounterRollupSerializer();
    public static AbstractSerializer<BluefloodEnumRollup> enumRollupInstance = new EnumRollupSerializer();

    static class Type {
        static final byte B_ROLLUP = (byte)'r';
        static final byte B_FLOAT_AS_DOUBLE = (byte)'f';
        static final byte B_ROLLUP_STAT = (byte)'t';
        static final byte B_COUNTER = (byte)'C';
        static final byte B_TIMER = (byte)'T';
        static final byte B_SET = (byte)'S';
        static final byte B_GAUGE = (byte)'G';
        static final byte B_ENUM = (byte)'E';
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
        else if (type.equals(BluefloodEnumRollup.class))
            return (AbstractSerializer<T>)enumRollupInstance;
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
            case Type.B_ENUM:
                sz += 1; // version
                BluefloodEnumRollup en = (BluefloodEnumRollup)o;
                Map<Long, Long> enValues = en.getHashedEnumValuesWithCounts();
                sz += CodedOutputStream.computeRawVarint32Size(en.getCount());
                for (Long enName  : enValues.keySet()) {
                    sz+=CodedOutputStream.computeRawVarint64Size(enName);
                    Long enValue = enValues.get(enName);
                    sz+= CodedOutputStream.computeRawVarint64Size(enValue);
                }
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
    
    public static class RawSerializer extends AbstractSerializer<Object> {

        private static RawSerDes serDes = new RawSerDes();

        @Override
        public ByteBuffer toByteBuffer(Object obj) {
            return serDes.serialize(obj);
        }

        @Override
        public Object fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
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
        private BasicRollupSerDes serDes = new BasicRollupSerDes();

        @Override
        public ByteBuffer toByteBuffer(BasicRollup basicRollup) {
            return serDes.serialize(basicRollup);
        }

        @Override
        public BasicRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }
    
    public static class TimerRollupSerializer extends AbstractSerializer<BluefloodTimerRollup> {

        private static TimerRollupSerDes serDes = new TimerRollupSerDes();

        @Override
        public ByteBuffer toByteBuffer(BluefloodTimerRollup timerRollup) {
            return serDes.serialize(timerRollup);
        }

        @VisibleForTesting
        public ByteBuffer toByteBufferWithV1Serialization(BluefloodTimerRollup timerRollup) {
            return serDes.serializeV1(timerRollup);
        }

        @Override
        public BluefloodTimerRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }
    
    public static class SetRollupSerializer extends AbstractSerializer<BluefloodSetRollup> {

        private static SetSerDes serDes = new SetSerDes();
        
        @Override
        public ByteBuffer toByteBuffer(BluefloodSetRollup setRollup) {
            return serDes.serialize(setRollup);
        }

        @Override
        public BluefloodSetRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }
    
    public static class GaugeRollupSerializer extends AbstractSerializer<BluefloodGaugeRollup> {

        private static GaugeSerDes serDes = new GaugeSerDes();

        @Override
        public ByteBuffer toByteBuffer(BluefloodGaugeRollup gaugeRollup) {
           return serDes.serialize(gaugeRollup);
        }

        @Override
        public BluefloodGaugeRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }

    public static class EnumRollupSerializer extends AbstractSerializer<BluefloodEnumRollup> {

        private static EnumSerDes serDes = new EnumSerDes();

        @Override
        public ByteBuffer toByteBuffer(BluefloodEnumRollup enumRollup) {
            return serDes.serialize(enumRollup);
        }

        @Override
        public BluefloodEnumRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }
    
    // for now let's try to get away with a single serializer for all single value rollups. We'll still encode specific
    // types so we can differentiate.
    public static class CounterRollupSerializer extends AbstractSerializer<BluefloodCounterRollup> {

        private static CounterSerDes serDes = new CounterSerDes();

        @Override
        public ByteBuffer toByteBuffer(BluefloodCounterRollup counterRollup) {
            return serDes.serialize(counterRollup);
        }

        @Override
        public BluefloodCounterRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }
}