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

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class StringMetadataSerializerTest {

    @Test
    public void testString() throws IOException {
        String[] values = {
            "abcdefg",
            "\u1234 \u0086 \uabcd \u5432",
            "ĆĐÈ¿ΔΞ€"
        };
        testRoundTrip(values);
    }

    private void testRoundTrip(String... strings) throws IOException {
        for (String str : strings) {
            byte[] buf = StringMetadataSerializer.get().toByteBuffer(str).array();
            ByteBuffer bb = ByteBuffer.wrap(buf);
                assertEquals(str, StringMetadataSerializer.get().fromByteBuffer(bb));
        }
    }
}