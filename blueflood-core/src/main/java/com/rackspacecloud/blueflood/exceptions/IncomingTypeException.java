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

package com.rackspacecloud.blueflood.exceptions;

import com.rackspacecloud.blueflood.exceptions.IncomingMetricException;
import com.rackspacecloud.blueflood.types.Locator;

public class IncomingTypeException extends IncomingMetricException {
    private final Locator locator;
    private final String oldType;
    private final String newType;
    
    public IncomingTypeException(Locator locator, String oldType, String newType) {
        super(String.format("Detected type change for %s %s->%s", locator.toString(), oldType, newType));
        this.locator = locator;
        this.oldType = oldType;
        this.newType = newType;
    }
}
