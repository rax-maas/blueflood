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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupEmissionWork implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(RollupEmissionWork.class);
    //TODO : Generalize this later for any event
    private String eventName = "rollup";
    private Object[] payload;

    protected RollupEmissionWork(Object... eventObjectsPayload) {
        this.payload = eventObjectsPayload;
    }

    @Override
    public void run() {
        try {
            RollupEventEmitter.getEmitterInstance().emit(eventName, payload);
        } catch (Exception e) {
            log.error("Error encountered while emitting to event ", eventName);
        }
    }
}
