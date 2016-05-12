/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.io.datastax;

import com.rackspacecloud.blueflood.io.serializers.metrics.CounterSerDes;
import com.rackspacecloud.blueflood.types.*;

import java.nio.ByteBuffer;

/**
 * This class holds the utility methods to read/write counter metrics
 * using Datastax driver.
 */
public class DCounterIO extends DAbstractMetricIO {

    private CounterSerDes serDes = new CounterSerDes();

    /**
     * Provides a way for the sub class to get a {@link java.nio.ByteBuffer}
     * representation of a certain Rollup object.
     *
     * @param value
     * @return
     */
    @Override
    protected ByteBuffer toByteBuffer(Object value) {
        if ( ! (value instanceof BluefloodCounterRollup) ) {
            throw new IllegalArgumentException("toByteBuffer(): expecting BluefloodCounterRollup class but got " + value.getClass().getSimpleName());
        }
        BluefloodCounterRollup counterRollup = (BluefloodCounterRollup) value;
        return serDes.serialize(counterRollup);
    }

    /**
     * Provides a way for the sub class to construct the right Rollup
     * object from a {@link java.nio.ByteBuffer}
     *
     * @param byteBuffer
     * @return
     */
    @Override
    protected BluefloodCounterRollup fromByteBuffer(ByteBuffer byteBuffer) {
        return serDes.deserialize(byteBuffer);
    }

}
