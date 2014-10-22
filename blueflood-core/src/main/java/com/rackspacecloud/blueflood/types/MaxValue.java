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

import com.rackspacecloud.blueflood.io.Constants;

public class MaxValue extends AbstractRollupStat {
    private boolean init;

    public MaxValue() {
        super();

        this.init = true;
        this.setDoubleValue(0.0);
        this.setLongValue(0);
    }

    @SuppressWarnings("unused") // used by Jackson
    public MaxValue(long value) {
        this();
        this.setLongValue(value);
    }

    @SuppressWarnings("unused") // used by Jackson
    public MaxValue(double value) {
        this();
        this.setDoubleValue(value);
    }

    @Override
    void handleFullResMetric(Object o) throws RuntimeException {
        if (o instanceof Double) {
            if (init) {
                this.setDoubleValue((Double)o);
                this.init = false;
                return;
            }

            if (!this.isFloatingPoint()) {
                if ((double)this.toLong() < (Double)o) {
                    this.setDoubleValue((Double)o);
                }
            } else {
                this.setDoubleValue(Math.max(this.toDouble(), (Double)o));
            }
        } else if (o instanceof Long || o instanceof Integer) {
            Long val;
            if (o instanceof Integer) {
                val = ((Integer)o).longValue();
            } else {
                val = (Long)o;
            }

            if (init) {
                this.setLongValue(val);
                this.init = false;
                return;
            }

            if (this.isFloatingPoint()) {
                double doubleValOther = val.doubleValue();
                if (this.toDouble()< doubleValOther) {
                    this.setLongValue(val);
                }
            } else {
                this.setLongValue(Math.max(this.toLong(), val));
            }
        } else {
            throw new RuntimeException("Unsuppored type " + o.getClass().getName() +" for min");
        }
    }

    @Override
    void handleRollupMetric(IBasicRollup basicRollup) throws RuntimeException {
        AbstractRollupStat other = basicRollup.getMaxValue();

        if (init) {
            if (other.isFloatingPoint()) {
                this.setDoubleValue(other.toDouble());
            } else {
                this.setLongValue(other.toLong());
            }

            this.init = false;
            return;
        }

        if (this.isFloatingPoint() && !other.isFloatingPoint()) {
            if (this.toDouble() < (double)other.toDouble()) {
                this.setLongValue(other.toLong());
            }
        } else if (!this.isFloatingPoint() && other.isFloatingPoint()) {
            if ((double)this.toLong() < other.toDouble()) {
                this.setDoubleValue(other.toDouble());
            }
        } else if (!this.isFloatingPoint() && !other.isFloatingPoint()) {
            this.setLongValue(Math.max(this.toLong(), other.toLong()));
        } else {
            this.setDoubleValue(Math.max(this.toDouble(), other.toDouble()));
        }
    }

    @Override
    public byte getStatType() {
        return Constants.MAX;
    }
}
