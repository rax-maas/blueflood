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

import com.rackspacecloud.blueflood.types.SimpleNumber;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;

public class SimpleNumberSerializer extends org.codehaus.jackson.map.JsonSerializer<com.rackspacecloud.blueflood.types.SimpleNumber> {
    @Override
    public void serialize(SimpleNumber value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        if (value.getDataType().equals(SimpleNumber.Type.INTEGER)) {
            jgen.writeNumber(value.getValue().intValue());
        } else if (value.getDataType().equals(SimpleNumber.Type.LONG)) {
            jgen.writeNumber(value.getValue().longValue());
        } else if (value.getDataType().equals(SimpleNumber.Type.DOUBLE)) {
            jgen.writeNumber(value.getValue().doubleValue());
        } else {
            throw new IOException("SimpleNumber must be an Integer Long or Double. Not " + value);
        }
    }
}
