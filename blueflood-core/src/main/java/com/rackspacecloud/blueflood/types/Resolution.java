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

package com.rackspacecloud.blueflood.types;

public enum Resolution {
    FULL(0),
    MIN5(1),
    MIN20(2),
    MIN60(3),
    MIN240(4),
    MIN1440(5);

    private final int value;

    private Resolution(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static Resolution fromString(String name) {
        return Resolution.valueOf(name.toUpperCase());
    }
}
