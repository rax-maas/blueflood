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

import com.rackspacecloud.blueflood.types.IMetric;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IMetricKafkaSerializer extends IMetricSerializer implements Encoder<IMetric>, Decoder<IMetric> {
    private static final Logger log = LoggerFactory.getLogger(IMetricKafkaSerializer.class);

    public IMetricKafkaSerializer(VerifiableProperties properties) {
        super();
    }

    @SuppressWarnings("unused")
    public byte[] toBytes(IMetric m) {
        try {
            return getObjectMapper().writeValueAsBytes(m);
        } catch (IOException e) {
            log.warn("IOException during serialization of " + m, e);
            return null;
        }
    }

    @SuppressWarnings("unused")
    public IMetric fromBytes(byte[] bytes) {
        ObjectMapper mapper = getObjectMapper();
        try {
            return mapper.readValue(bytes, IMetric.class);
        } catch (IOException e) {
            log.warn("IOException during deserialization of " + bytes, e);
            return null;
        }
    }
}
