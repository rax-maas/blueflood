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

package com.rackspacecloud.blueflood.eventemitter;

import com.github.nkzawa.emitter.Emitter;
import junit.framework.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;

public class RollupEventEmitterTest {
    String testEventName = "test";
    EventListener elistener = new EventListener();
    ArrayList<Object> store = new ArrayList<Object>();

    @Test
    public void testConcurrentEmission() throws Exception {
        //Test subscription
        RollupEventEmitter.getEmitterInstance().on(testEventName,elistener);
        Assert.assertTrue(RollupEventEmitter.getEmitterInstance().listeners(testEventName).contains(elistener));

        //Test concurrent emission
        final String obj1 = "payload1";
        final String obj2 = "payload2";
        final CountDownLatch startLatch = new CountDownLatch(1);
        Future<Object> f1 = RollupEventEmitter.getEventExecutors().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                startLatch.await();
                RollupEventEmitter.getEmitterInstance().emit(testEventName, obj1);
                return null;
            }
        });
        Future<Object> f2 = RollupEventEmitter.getEventExecutors().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                startLatch.await();
                RollupEventEmitter.getEmitterInstance().emit(testEventName, obj2);
                return null;
            }
        });
        startLatch.countDown();
        f1.get();
        f2.get();
        Assert.assertEquals(store.size(),2);
        Assert.assertTrue(store.contains(obj1));
        Assert.assertTrue(store.contains(obj2));

        //Test unsubscription
        RollupEventEmitter.getEmitterInstance().off(testEventName, elistener);
        Assert.assertFalse(RollupEventEmitter.getEmitterInstance().listeners(testEventName).contains(elistener));
    }

    private class EventListener implements Emitter.Listener {
        @Override
        public void call(Object... objects) {
            store.addAll(Arrays.asList(objects));
        }
    }
}
