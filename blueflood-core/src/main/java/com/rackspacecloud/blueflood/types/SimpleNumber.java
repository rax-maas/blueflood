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

public class SimpleNumber implements Rollup {
    private final Number value;
    private final Type type;

    @Override
    public Boolean hasData() {
        return true; // value cannot be null, therefore this is always true.
    }

    public enum Type {
        INTEGER,
        LONG,
        DOUBLE
    }

    public SimpleNumber(Object value) {
        if (value == null)
            throw new NullPointerException("value cannot be null");
        if (value instanceof Integer) {
            this.type = Type.INTEGER;
            this.value = (Number)value;
        } else if (value instanceof Long) {
            this.type = Type.LONG;
            this.value = (Number)value;
        } else if (value instanceof Double) {
            this.type = Type.DOUBLE;
            this.value = (Number)value;
        } else if (value instanceof SimpleNumber) {
            this.type = ((SimpleNumber)value).type;
            this.value = ((SimpleNumber)value).value;
        } else {
            throw new IllegalArgumentException("Unexpected argument type " + value.getClass() + ", expected number.");
        }
    }

    public Number getValue() {
        return value;
    }

    public Type getDataType() {
        return type;
    }

    public String toString() {
        switch (type) {
            case INTEGER:
                return String.format("%d (int)", value.intValue());
            case LONG:
                return String.format("%d (long)", value.longValue());
            case DOUBLE:
                return String.format("%s (double)", value.toString());
            default:
                return super.toString();
        }
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.NOT_A_ROLLUP;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SimpleNumber))
            return false;
        SimpleNumber other = (SimpleNumber)obj;
        return other.value == this.value || other.value.equals(this.value);
    }
}
