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

import com.bigml.histogram.Bin;
import com.bigml.histogram.SimpleTarget;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.HistogramRollup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

public class HistogramSerializer extends AbstractSerializer<HistogramRollup> {
    private static final HistogramSerializer INSTANCE = new HistogramSerializer();

    public static HistogramSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(HistogramRollup histogramRollup) {
        final Collection<Bin<SimpleTarget>> bins = filterZeroCountBins(histogramRollup.getBins());
        byte[] buf = new byte[computeSizeOfHistogramRollupOnDisk(bins)];
        try {
            serializeBins(bins, buf);
            return ByteBuffer.wrap(buf);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public HistogramRollup fromByteBuffer(ByteBuffer byteBuffer) {
        CodedInputStream in = CodedInputStream.newInstance(byteBuffer.array());

        try {
            byte version = in.readRawByte();
            switch (version) {
                case Constants.VERSION_1_HISTOGRAM:
                    return deserializeV1Histogram(in);
                default:
                    throw new SerializationException("Unexpected serialization version");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex) ;
        }
    }

    private HistogramRollup deserializeV1Histogram(CodedInputStream in) throws IOException {
        final Collection<Bin<SimpleTarget>> bins = new ArrayList<Bin<SimpleTarget>>();

        while (!in.isAtEnd()) {
            long count = in.readRawVarint64();
            double mean = in.readDouble();
            Bin<SimpleTarget> bin = new Bin<SimpleTarget>(mean, count, SimpleTarget.TARGET);
            bins.add(bin);
        }

        return new HistogramRollup(bins);
    }

    private void serializeBins(Collection<Bin<SimpleTarget>> bins, byte[] buf) throws IOException {
        CodedOutputStream protobufOut = CodedOutputStream.newInstance(buf);

        protobufOut.writeRawByte(Constants.VERSION_1_HISTOGRAM);

        for (Bin<SimpleTarget> bin : bins) {
            protobufOut.writeRawVarint64((long) bin.getCount());
            protobufOut.writeDoubleNoTag(bin.getMean());
        }
    }

    private int computeSizeOfHistogramRollupOnDisk(Collection<Bin<SimpleTarget>> bins) {
        int size = 1; // for version

        for (Bin<SimpleTarget> bin : bins) {
            size += CodedOutputStream.computeDoubleSizeNoTag((bin.getMean()));
            size += CodedOutputStream.computeRawVarint64Size((long) bin.getCount());
        }

        return size;
    }

    private Collection<Bin<SimpleTarget>> filterZeroCountBins(Collection<Bin<SimpleTarget>> bins) {
        Collection<Bin<SimpleTarget>> filtered = new ArrayList<Bin<SimpleTarget>>();
        for (Bin<SimpleTarget> bin : bins) {
            if (bin.getCount() > 0)  {
                filtered.add(bin);
            }
        }

        return filtered;
    }
}
