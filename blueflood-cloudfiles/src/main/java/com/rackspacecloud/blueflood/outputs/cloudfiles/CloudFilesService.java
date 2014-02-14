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

package com.rackspacecloud.blueflood.outputs.cloudfiles;

import com.rackspacecloud.blueflood.eventemitter.Emitter;
import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.eventemitter.RollupEventEmitter;
import com.rackspacecloud.blueflood.service.EventListenerService;
import org.slf4j.Logger;

import java.io.IOException;

public class CloudFilesService  implements Emitter.Listener<RollupEvent>, EventListenerService {
    private Boolean started = false;
    private final RollupEventEmitter eventEmitter = RollupEventEmitter.getInstance();
    private StorageManager storageManager;
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CloudFilesService.class);


    @Override
    public synchronized void startService() {
        if (started) { throw new RuntimeException("Already started, but startService was called"); }
        try {
            storageManager = new StorageManager();
        } catch (IOException e) {
            log.error("Error starting up Cloud Files exporter", e);
        }

        storageManager.start();
        //Register with the event emitter
        eventEmitter.on(RollupEventEmitter.ROLLUP_EVENT_NAME, this);
        log.debug("Listening for rollup events.");

        started = true;
    }

    @Override
    public synchronized void stopService() {
        if (!started) { throw new RuntimeException("Not started, but stopService was called"); }
        try {
            storageManager.stop();
            started = false;
        } catch (IOException e) {
            log.warn("Error shutting down CloudFilesPublisher", e);
        }
        eventEmitter.off(RollupEventEmitter.ROLLUP_EVENT_NAME, this);
    }

    @Override
    public void call(RollupEvent... args) {
        try {
            storageManager.store(args);
        } catch (IOException e) {
            log.error("Problem enqueueing rollup events for upload.", e);
        }
    }
}
