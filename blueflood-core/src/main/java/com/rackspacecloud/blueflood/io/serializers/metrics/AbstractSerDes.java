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
import com.rackspacecloud.blueflood.types.*;

import java.io.IOException;

/**
 * This is an abstract class that most Serialization/Deserialization class
 * extends from. It contains some utility methods.
 */
public abstract class AbstractSerDes {

    protected static StatsSerDes<Average> averageStatDeSer = new StatsSerDes<Average>();
    protected static StatsSerDes<MinValue> minStatDeSer = new StatsSerDes<MinValue>();
    protected static StatsSerDes<MaxValue> maxStatDeSer = new StatsSerDes<MaxValue>();
    protected static StatsSerDes<Variance> varianceStatDeSer = new StatsSerDes<Variance>();

    protected void putRollupStat(AbstractRollupStat stat, CodedOutputStream protobufOut) throws IOException {
        protobufOut.writeRawByte(stat.getStatType());   // stat type
        protobufOut.writeRawByte(stat.isFloatingPoint() ? Constants.B_DOUBLE : Constants.B_I64);

        if (stat.isFloatingPoint()) {
            protobufOut.writeDoubleNoTag(stat.toDouble());
        } else {
            protobufOut.writeRawVarint64(stat.toLong());
        }
    }

    // these two sizeOf*() methods are created mainly
    // for code readability in the subclasses

    /**
     * @return the number of byte(s) used in the serialized byte buffer
     * to indicate the size of the byte buffer. The size of the byte
     * buffer is the first byte in the buffer.
     */
    protected final int sizeOfSize() {
        return 1;
    }

    /**
     * @return the number of byte(s) used in the serialized byte buffer
     * to indicate the type of the object being serialized. The type
     * of the object is the second byte in the buffer.
     */
    protected final int sizeOfType() {
        return 1;
    }

    // read out a type-specified number.
    protected Number getUnversionedDoubleOrLong(CodedInputStream in) throws IOException {
        byte type = in.readRawByte();
        if (type == Constants.B_DOUBLE)
            return in.readDouble();
        else
            return in.readRawVarint64();
    }

    // put out a number prefaced only by a type.
    protected void putUnversionedDoubleOrLong(Number number, CodedOutputStream out) throws IOException {
        if (number instanceof Double) {
            out.writeRawByte(Constants.B_DOUBLE);
            out.writeDoubleNoTag(number.doubleValue());
        } else {
            out.writeRawByte(Constants.B_I64);
            out.writeRawVarint64(number.longValue());
        }
    }
}
