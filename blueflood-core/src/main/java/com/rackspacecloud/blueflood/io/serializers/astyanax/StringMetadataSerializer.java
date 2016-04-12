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
package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.rackspacecloud.blueflood.io.serializers.metrics.StringMetadataSerDes;

import java.nio.ByteBuffer;

public class StringMetadataSerializer extends AbstractSerializer<String> {
    private static final StringMetadataSerDes deSer = new StringMetadataSerDes();
    private static final StringMetadataSerializer INSTANCE = new StringMetadataSerializer();

    // However, we never actually used that functionality, and it duplicated a lot of logic
    // as is contained in NumericSerializer, without actually being in a compatible [de]serialization format.
    // TODO: re-add non-str functionality in a way that does not involve as much code duplication and format incompatibility

    public static StringMetadataSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(String str) {
        return deSer.serialize(str);
    }

    @Override
    public String fromByteBuffer(ByteBuffer byteBuffer) {
        return deSer.deserialize(byteBuffer);
    }

    // prevent people from instantiating this class
    private StringMetadataSerializer() {
    }
}
