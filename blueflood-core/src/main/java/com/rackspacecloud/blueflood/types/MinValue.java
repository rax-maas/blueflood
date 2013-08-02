package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;

public class MinValue extends AbstractRollupStat {
    private boolean init;

    public MinValue() {
        super();

        this.init = true;
        this.setDoubleValue(0.0d);
        this.setLongValue(0);
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
                if ((double)this.toLong() > (Double)o) {
                    this.setDoubleValue((Double)o);
                }
            } else {
                this.setDoubleValue(Math.min(this.toDouble(), (Double)o));
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
                if (this.toDouble()> doubleValOther) {
                    this.setLongValue((Long)o);
                }
            } else {
                this.setLongValue(Math.min(this.toLong(), val));
            }
        } else {
            throw new RuntimeException("Unsuppored type " + o.getClass().getName() +" for min");
        }
    }

    @Override
    void handleRollupMetric(Rollup rollup) throws RuntimeException {
        MinValue other = rollup.getMinValue();

        if (init) {
            if (other.isFloatingPoint()) {
                this.setDoubleValue(other.toDouble());
            } else {
                this.setLongValue(other.toLong());
            }

            init = false;
            return;
        }

        if (this.isFloatingPoint() && !other.isFloatingPoint()) {
            if (this.toDouble() > (double)other.toLong()) {
                this.setLongValue(other.toLong());
            }
        } else if (!this.isFloatingPoint() && other.isFloatingPoint()) {
            if ((double)this.toLong()> other.toDouble()) {
                this.setDoubleValue(other.toDouble());
            }
        } else if (!this.isFloatingPoint() && !other.isFloatingPoint()) {
            this.setLongValue(Math.min(this.toLong(), other.toLong()));
        } else {
            this.setDoubleValue(Math.min(this.toDouble(), other.toDouble()));
        }
    }

    @Override
    public byte getStatType() {
        return Constants.MIN;
    }
}