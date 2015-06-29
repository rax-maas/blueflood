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

package com.rackspacecloud.blueflood.outputs.serializers;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.io.Constants;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;

public class KafkaRollupSerializer implements Encoder<RollupEvent>, Decoder<RollupEvent> {
    private static final Logger log = LoggerFactory.getLogger(KafkaRollupSerializer.class);

    //When custom serializer is loaded by the kafka producer, it expects this constructor
    public KafkaRollupSerializer(VerifiableProperties properties) {

    }

    @Override
    public RollupEvent fromBytes(byte[] bytes) {
        //TODO :  Decide on requirements of consumer side and change accordingly
        return null;
    }

    @Override
    public byte[] toBytes(RollupEvent rollupPayload) {
        try {
            return RollupEventSerializer.serializeRollupEvent(rollupPayload).toString().getBytes(Constants.DEFAULT_CHARSET);
        } catch (Exception e) {
            log.error("Error encountered while serializing RollupEvent JSON to bytes: ", e);
            throw new IOError(e);
        }
    }
}
