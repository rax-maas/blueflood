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

package com.rackspacecloud.blueflood.utils;

import java.util.concurrent.TimeUnit;

public class TimeValue {
    private final TimeUnit unit;
    private final long value;

    public TimeValue(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public long getValue() {
        return this.value;
    }

    public TimeUnit getUnit() {
        return this.unit;
    }

    public long toDays() {
        return this.unit.toDays(this.value);
    }

    public long toHours() {
        return this.unit.toHours(this.value);
    }

    public long toMinutes() {
        return this.unit.toMinutes(this.value);
    }

    public long toSeconds() {
        return this.unit.toSeconds(this.value);
    }

    public long toMillis() {
        return this.unit.toMillis(this.value);
    }

    public long toMicros() {
        return this.unit.toMicros(this.value);
    }

    public String toString() {
        return String.format("%s %s", String.valueOf(this.getValue()), unit.name());
    }

    public boolean equals(TimeValue other) {
        if (other == null) return false;

        return other.getValue() == this.getValue() && other.getUnit().equals(this.getUnit());
    }
}