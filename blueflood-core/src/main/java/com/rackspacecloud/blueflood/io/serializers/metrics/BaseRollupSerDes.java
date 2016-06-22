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
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.BaseRollup;
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.IOException;

/**
 * The serialization and deserialization methods for sub-metrics used by
 * {@link com.rackspacecloud.blueflood.types.BasicRollup} and {@link com.rackspacecloud.blueflood.types.BluefloodGaugeRollup}.
 * Common sub-metrics are:
 * <ul>
 *     <li>average</li>
 *     <li>max</li>
 *     <li>min</li>
 *     <li>sum</li>
 * </ul>
 */
public class BaseRollupSerDes extends AbstractSerDes {

    /**
     * Our internal metric to count the number of rollups
     */
    protected static Histogram rollupSize = Metrics.histogram( BasicRollupSerDes.class, "Rollup Metric Size" );

    protected int sizeOfBaseRollup( BaseRollup baseRollup ) {
        int sz = sizeOfSize();

        sz += CodedOutputStream.computeRawVarint64Size( baseRollup.getCount() );
        if ( baseRollup.getCount() > 0 ) {
            sz += averageStatDeSer.sizeOf( baseRollup.getAverage() );
            sz += varianceStatDeSer.sizeOf( baseRollup.getVariance() );
            sz += minStatDeSer.sizeOf( baseRollup.getMinValue() );
            sz += maxStatDeSer.sizeOf( baseRollup.getMaxValue() );
        }
        return sz;
    }

    /**
     * Serialize without sum attribute.  This is used by {@link GaugeSerDes}
     *
     * @param baseRollup
     * @param protobufOut
     * @throws IOException
     */
    protected void serializeRollupV1( BaseRollup baseRollup, CodedOutputStream protobufOut ) throws IOException {
        protobufOut.writeRawByte( Constants.VERSION_1_ROLLUP );
        serializeBaseRollupHelper( baseRollup, protobufOut );
    }

    protected void serializeBaseRollupHelper( BaseRollup baseRollup, CodedOutputStream protobufOut ) throws IOException {
        protobufOut.writeRawVarint64( baseRollup.getCount() );          // stat count

        if ( baseRollup.getCount() > 0 ) {
            putRollupStat( baseRollup.getAverage(), protobufOut );
            putRollupStat( baseRollup.getVariance(), protobufOut );
            putRollupStat( baseRollup.getMinValue(), protobufOut );
            putRollupStat( baseRollup.getMaxValue(), protobufOut );
        }
    }

    protected void deserializeBaseRollup( BaseRollup baseRollup, CodedInputStream in, byte version ) throws IOException {

        final long count = in.readRawVarint64();
        baseRollup.setCount(count);

        if (count <= 0) {
            return;
        }

        for ( int i = 0; i < BaseRollup.NUM_STATS; i++ ) {
            byte statType = in.readRawByte();
            switch ( statType ) {
                case Constants.AVERAGE:
                    averageStatDeSer.deserialize( baseRollup.getAverage(), in );
                    break;
                case Constants.VARIANCE:
                    varianceStatDeSer.deserialize( baseRollup.getVariance(), in );
                    break;
                case Constants.MIN:
                    minStatDeSer.deserialize( baseRollup.getMinValue(), in );
                    break;
                case Constants.MAX:
                    maxStatDeSer.deserialize( baseRollup.getMaxValue(), in );
                    break;
                default:
                    throw new SerializationException( "invalid stat " + (int) version + " type: " + (int) statType );
            }

        }
    }
}
