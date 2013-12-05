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

import junit.framework.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;

public class RollupEventEmitterTest {
    String testEventName = "test";
    EventListener elistener = new EventListener();
    ArrayList<RollupEmission> store = new ArrayList<RollupEmission>();

    @Test
    public void testConcurrentEmission() throws Exception {
        //Test subscription
        RollupEventEmitter.getInstance().on(testEventName,elistener);
        Assert.assertTrue(RollupEventEmitter.getInstance().listeners(testEventName).contains(elistener));
        Assert.assertTrue(store.isEmpty());

        //Test concurrent emission
        final RollupEmission obj1 = new RollupEmission(null, null, "payload1");
        final RollupEmission obj2 = new RollupEmission(null, null, "payload2");
        final CountDownLatch startLatch = new CountDownLatch(1);
        Future<Object> f1 = RollupEventEmitter.getEventExecutors().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                startLatch.await();
                RollupEventEmitter.getInstance().emit(testEventName, obj1);
                return null;
            }
        });
        Future<Object> f2 = RollupEventEmitter.getEventExecutors().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                startLatch.await();
                RollupEventEmitter.getInstance().emit(testEventName, obj2);
                return null;
            }
        });
        startLatch.countDown();
        f1.get();
        f2.get();

        Thread.sleep(1000);

        Assert.assertEquals(store.size(),2);
        Assert.assertTrue(store.contains(obj1));
        Assert.assertTrue(store.contains(obj2));

        //Test unsubscription
        RollupEventEmitter.getInstance().off(testEventName, elistener);
        Assert.assertFalse(RollupEventEmitter.getInstance().listeners(testEventName).contains(elistener));

        //Clear the store and check if it is not getting filled again
        store.clear();
        Future<Object> f3 = RollupEventEmitter.getEventExecutors().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                RollupEventEmitter.getInstance().emit(testEventName, obj1);
                return null;
            }
        });
        f3.get();

        Thread.sleep(1000);

        Assert.assertTrue(store.isEmpty());
    }

    private class EventListener implements Emitter.Listener<RollupEmission> {
        @Override
        public void call(RollupEmission... rollupEventObjects) {
            store.addAll(Arrays.asList(rollupEventObjects));
        }
    }
}
