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

public class SimpleNumber extends Rollup {
    private int intValue;
    private double doubleValue;
    private long longValue;
    private Type type;

    enum Type {
        INTEGER("I"),
        LONG("L"),
        DOUBLE("D");

        private Type(String rep) {
            typeVal = rep;
        }

        private final String typeVal;
    }

    public SimpleNumber(Object value) {
        if (value instanceof Integer) {
            this.intValue = (Integer) value;
            this.type = Type.INTEGER;
        } else if (value instanceof Long) {
            this.longValue = (Long) value;
            this.type = Type.LONG;
        } else if (value instanceof Double) {
            this.doubleValue = (Double) value;
            this.type = Type.DOUBLE;
        } else {
            throw new IllegalArgumentException("Unexpected argument type " + value.getClass() + ", expected number.");
        }
    }

    public boolean isFloatingPoint() {
        return this.type.equals(Type.DOUBLE);
    }

    public Object getValue() {
        if (type.equals(Type.LONG)) {
            return longValue;
        } else if (type.equals(Type.INTEGER)) {
            return intValue;
        } else {
            return doubleValue;
        }
    }
}
