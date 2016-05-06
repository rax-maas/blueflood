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
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.io.serializers.metrics.*;
import com.rackspacecloud.blueflood.types.*;

import java.nio.ByteBuffer;

public class Serializers {
    // NumericSerializer can be used with Rollup and full resolution metrics.

    public static final SimpleNumberSerializer simpleNumberSerializer = new SimpleNumberSerializer();
    private static RawSerializer fullInstance = new RawSerializer();
    private static BasicRollupSerializer basicRollupInstance = new BasicRollupSerializer();
    public static TimerRollupSerializer timerRollupInstance = new TimerRollupSerializer();
    public static SetRollupSerializer setRollupInstance = new SetRollupSerializer();
    public static GaugeRollupSerializer gaugeRollupInstance = new GaugeRollupSerializer();
    public static CounterRollupSerializer counterRollupInstance = new CounterRollupSerializer();
    public static EnumRollupSerializer enumRollupInstance = new EnumRollupSerializer();

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
        else if (type.equals(BluefloodCounterRollup.class))
            return (AbstractSerializer<T>) counterRollupInstance;
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
    
    public static class RawSerializer extends AbstractSerializer<Object> {

        private static RawSerDes serDes = new RawSerDes();

        // prevent people from instantiating this class
        private RawSerializer() {
        }

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

        // prevent people from instantiating this class
        private SimpleNumberSerializer() {
        }
        
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

        // prevent people from instantiating this class
        private BasicRollupSerializer() {
        }

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

        // prevent people from instantiating this class
        private TimerRollupSerializer() {
        }

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

        // prevent people from instantiating this class
        private SetRollupSerializer() {
        }
        
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

        // prevent people from instantiating this class
        private GaugeRollupSerializer() {
        }

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

        // prevent people from instantiating this class
        private EnumRollupSerializer() {
        }

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

        // prevent people from instantiating this class
        private CounterRollupSerializer() {
        }

        @Override
        public ByteBuffer toByteBuffer(BluefloodCounterRollup counterRollup) {
            return serDes.serialize(counterRollup);
        }

        @Override
        public BluefloodCounterRollup fromByteBuffer(ByteBuffer byteBuffer) {
            return serDes.deserialize(byteBuffer);
        }
    }

    // prevent people from instantiating this class
    private Serializers() {
    }
}