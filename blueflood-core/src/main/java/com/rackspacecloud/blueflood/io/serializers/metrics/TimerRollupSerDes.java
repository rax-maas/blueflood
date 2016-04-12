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
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.rackspacecloud.blueflood.io.Constants.VERSION_1_TIMER;
import static com.rackspacecloud.blueflood.io.Constants.VERSION_2_TIMER;

/**
 * This class knows how to serialize/deserialize Timer metrics.
 */
public class TimerRollupSerDes extends AbstractSerDes {

    private static Histogram timerRollupSize = Metrics.histogram(TimerRollupSerDes.class, "Timer Metric Size");

    public ByteBuffer serialize(BluefloodTimerRollup bluefloodTimerRollup) {
        try {
            byte[] buf = new byte[sizeOf(bluefloodTimerRollup, VERSION_2_TIMER)];
            serializeTimer(bluefloodTimerRollup, buf, VERSION_2_TIMER);
            return ByteBuffer.wrap(buf);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @VisibleForTesting
    public ByteBuffer serializeV1(BluefloodTimerRollup bluefloodTimerRollup) {
        try {
            byte[] buf = new byte[sizeOf(bluefloodTimerRollup, VERSION_1_TIMER)];
            serializeTimer(bluefloodTimerRollup, buf, VERSION_1_TIMER);
            return ByteBuffer.wrap(buf);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public BluefloodTimerRollup deserialize(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());
        try {
            byte version = in.readRawByte();
            return deserializeTimer(in, version);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void serializeTimer(BluefloodTimerRollup rollup, byte[] buf, byte timerVersion) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        timerRollupSize.update(buf.length);
        out.writeRawByte(timerVersion);

        // sum, count, countps, avg, max, min, var
        if (timerVersion == VERSION_1_TIMER) {
            out.writeRawVarint64((long)rollup.getSum());
        } else if (timerVersion == VERSION_2_TIMER) {
            out.writeDoubleNoTag(rollup.getSum());
        } else {
            throw new SerializationException(String.format("Unexpected timer serialization version: %d", (int)timerVersion));
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

    private BluefloodTimerRollup deserializeTimer(CodedInputStream in, byte timerVersion) throws IOException {
        // note: type and version have already been read.
        final double sum;
        if (timerVersion == VERSION_1_TIMER) {
            sum = in.readRawVarint64();
        } else if (timerVersion == VERSION_2_TIMER) {
            sum = in.readDouble();
        } else {
            throw new SerializationException(String.format("Unexpected timer deserialization version: %d", (int)timerVersion));
        }

        final long count = in.readRawVarint64();
        final double countPs = in.readDouble();
        final int sampleCount = in.readRawVarint32();

        // average
        byte statType = in.readRawByte();
        Average average = new Average();
        averageStatDeSer.deserialize(average, in);

        // max
        statType = in.readRawByte();
        MaxValue maxValue = new MaxValue();
        maxStatDeSer.deserialize(maxValue, in);

        // min
        statType = in.readRawByte();
        MinValue minValue = new MinValue();
        minStatDeSer.deserialize(minValue, in);

        // var
        statType = in.readRawByte();
        Variance variance = new Variance();
        varianceStatDeSer.deserialize(variance, in);

        BluefloodTimerRollup rollup = new BluefloodTimerRollup()
                .withSum(sum)
                .withCount(count)
                .withCountPS(countPs)
                .withSampleCount(sampleCount)
                .withAverage(average)
                .withMaxValue(maxValue)
                .withMinValue(minValue)
                .withVariance(variance);

        int numPercentiles = in.readRawVarint32();
        for (int i = 0; i < numPercentiles; i++) {
            String name = in.readString();
            Number mean = getUnversionedDoubleOrLong(in);
            rollup.setPercentile(name, mean);
        }

        return rollup;
    }

    private int sizeOf(BluefloodTimerRollup bluefloodTimerRollup, byte timerVersion) throws SerializationException {

        int sz = sizeOfSize();

        if (timerVersion == VERSION_1_TIMER) {
            sz += CodedOutputStream.computeRawVarint64Size((long) bluefloodTimerRollup.getSum());
        } else if (timerVersion == VERSION_2_TIMER) {
            sz += CodedOutputStream.computeDoubleSizeNoTag(bluefloodTimerRollup.getSum());
        } else {
            throw new SerializationException(String.format("Unexpected timer serialization version: %d", (int)timerVersion));
        }
        sz += CodedOutputStream.computeRawVarint64Size(bluefloodTimerRollup.getCount());
        sz += CodedOutputStream.computeDoubleSizeNoTag(bluefloodTimerRollup.getRate());
        sz += CodedOutputStream.computeRawVarint32Size(bluefloodTimerRollup.getSampleCount());
        sz += averageStatDeSer.sizeOf(bluefloodTimerRollup.getAverage());
        sz += maxStatDeSer.sizeOf(bluefloodTimerRollup.getMaxValue());
        sz += minStatDeSer.sizeOf(bluefloodTimerRollup.getMinValue());
        sz += varianceStatDeSer.sizeOf(bluefloodTimerRollup.getVariance());

        Map<String, BluefloodTimerRollup.Percentile> percentiles = bluefloodTimerRollup.getPercentiles();
        sz += CodedOutputStream.computeRawVarint32Size(bluefloodTimerRollup.getPercentiles().size());
        for (Map.Entry<String, BluefloodTimerRollup.Percentile> entry : percentiles.entrySet()) {
            sz += CodedOutputStream.computeStringSizeNoTag(entry.getKey());
            Number[] pctComponents = new Number[] {
                    entry.getValue().getMean(),
            };
            for (Number num : pctComponents) {
                sz += sizeOfType();
                if (num instanceof Long || num instanceof Integer) {
                    sz += CodedOutputStream.computeRawVarint64Size(num.longValue());
                } else if (num instanceof Double || num instanceof Float) {
                    sz += CodedOutputStream.computeDoubleSizeNoTag(num.doubleValue());
                }
            }
        }
        return sz;
    }

}
