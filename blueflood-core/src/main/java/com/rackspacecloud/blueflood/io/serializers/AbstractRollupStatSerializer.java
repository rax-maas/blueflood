/*
 * Copyright 2014 Rackspace
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

import com.rackspacecloud.blueflood.types.AbstractRollupStat;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;

public class AbstractRollupStatSerializer extends org.codehaus.jackson.map.JsonSerializer<com.rackspacecloud.blueflood.types.AbstractRollupStat> {
    @Override
    public void serialize(AbstractRollupStat value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (value.isFloatingPoint()) {
            jgen.writeNumber(value.toDouble());
        } else {
            jgen.writeNumber(value.toLong());
        }
    }
}