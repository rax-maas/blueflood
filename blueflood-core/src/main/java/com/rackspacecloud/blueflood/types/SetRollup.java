package com.rackspacecloud.blueflood.types;


public class SetRollup extends SingleValueRollup {
    
    public SetRollup withCount(long count) {
        return (SetRollup) this.withValue(count);
    }
}
