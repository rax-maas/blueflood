package com.rackspacecloud.blueflood.types;


public class SetRollup extends SingleValueRollup {
    
    public SetRollup withCount(long count) {
        return (SetRollup) this.withValue(count);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SetRollup))
            return false;
        else
            return getValue().equals(((SetRollup)obj).getValue());
    }
}
