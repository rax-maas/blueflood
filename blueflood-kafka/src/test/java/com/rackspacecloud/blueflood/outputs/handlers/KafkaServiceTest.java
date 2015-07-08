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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.eventemitter.RollupEventEmitter;
import com.rackspacecloud.blueflood.types.BasicRollup;
import junit.framework.Assert;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.junit.Test;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

public class KafkaServiceTest {
    String eventName = "rollup";
    RollupEvent rollupEvent = new RollupEvent(null, new BasicRollup(), "payload", "metrics_1440m", 0);

    @Test
    public void testKafkaService() throws Exception {

        KafkaService kafkaService = new KafkaService();
        KafkaService kafkaServiceSpy = spy(kafkaService);
        //Create a mock producer object and add it to the producer list
        Producer mockProducer = mock(Producer.class);
        ArrayList<Producer> producerList = kafkaServiceSpy.getProducerListUnsafe();
        producerList.clear();
        producerList.add(mockProducer);

        //Start KafkaProduction and test whether listener object was added
        kafkaServiceSpy.startService();
        Assert.assertTrue(RollupEventEmitter.getInstance().listeners(eventName).contains(kafkaServiceSpy));

        //Emit an event.
        RollupEventEmitter.getInstance().emit(eventName, rollupEvent);
        //Verify that the call method was called atleast once
        verify(kafkaServiceSpy, timeout(1000).atLeastOnce()).call(rollupEvent);
        //Verify that there were interactions with the mock producer
        verify(mockProducer, timeout(1000)).send(anyListOf(KeyedMessage.class));

        //Stop Kafka Production and test whether the listener object was removed
        kafkaServiceSpy.stopService();
        Assert.assertFalse(RollupEventEmitter.getInstance().listeners(eventName).contains(kafkaServiceSpy));

        //Reset mocks, emit event and check if methods are not called
        reset(kafkaServiceSpy, mockProducer);
        RollupEventEmitter.getInstance().emit(eventName, rollupEvent);
        verifyZeroInteractions(kafkaServiceSpy);
        verifyZeroInteractions(mockProducer);
    }
}
