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
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.AbstractRollupStat;

import java.io.IOException;

/**
 * This is the generics serializer for Average, Min, Max, Variance stats (aka sub-stats).
 */
public class StatsSerDes<T extends AbstractRollupStat> {

    public void deserialize(T stat, CodedInputStream in) throws IOException {

        byte metricValueType = in.readRawByte();
        switch(metricValueType) {
            case Constants.I64:
                stat.setLongValue(in.readRawVarint64());
                break;
            case Constants.B_DOUBLE:
                stat.setDoubleValue(in.readDouble());
                break;
            default:
                throw new IOException("Unsupported stat value type " + (int) metricValueType);
        }
    }

    public int sizeOf(T stat) {
        int sz = 1 + 1; // type + isFP.
        sz += stat.isFloatingPoint() ?
                CodedOutputStream.computeDoubleSizeNoTag(stat.toDouble()) :
                CodedOutputStream.computeRawVarint64Size(stat.toLong());
        return sz;
    }
}
