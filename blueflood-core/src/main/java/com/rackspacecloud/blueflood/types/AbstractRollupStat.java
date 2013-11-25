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

import com.rackspacecloud.blueflood.utils.Util;

public abstract class AbstractRollupStat {
    private long longValue;
    private double doubleValue;
    private boolean isFloatingPoint;

    public AbstractRollupStat() {
        this.longValue = 0;
        this.doubleValue = 0;
        this.isFloatingPoint = false;
    }

    public boolean isFloatingPoint() {
        return this.isFloatingPoint;
    }

    public double toDouble() {
        return this.doubleValue;
    }

    public long toLong() {
        return this.longValue;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof AbstractRollupStat)) {
            return false;
        }

        AbstractRollupStat other = (AbstractRollupStat)otherObject;

        if (this.isFloatingPoint != other.isFloatingPoint()) {
            return false;
        }

        if (this.isFloatingPoint) {
            return this.toDouble() == other.toDouble();
        } else {
            return this.toLong() == other.toLong();
        }
    }

    public void setLongValue(long value) {
        this.isFloatingPoint = false;
        this.longValue = value;
    }

    public void setDoubleValue(double value) {
        this.isFloatingPoint = true;
        this.doubleValue = value;
    }
    
    abstract void handleFullResMetric(Object o) throws RuntimeException;
    abstract void handleRollupMetric(IBasicRollup basicRollup) throws RuntimeException;
    abstract public byte getStatType();
    
    public String toString() {
        if (isFloatingPoint)
            return Util.DECIMAL_FORMAT.format(doubleValue);
        else
            return Long.toString(longValue);
    }
    
    public static void set(AbstractRollupStat stat, Number value) {
        if (value instanceof Long)
            stat.setLongValue(value.longValue());
        else if (value instanceof Double)
            stat.setDoubleValue(value.doubleValue());
        else if (value instanceof Integer)
            stat.setLongValue(value.longValue());
        else if (value instanceof Float)
            stat.setDoubleValue(value.doubleValue());
        else
            throw new ClassCastException(String.format("%s cannot be set to AbstractRollupState.value", value.getClass().getName()));
    }
}