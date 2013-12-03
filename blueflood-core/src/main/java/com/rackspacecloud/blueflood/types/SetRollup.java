package com.rackspacecloud.blueflood.types;


import java.io.IOException;

public class SetRollup extends BasicRollup {
    
    public SetRollup() {}
    
    public static SetRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
        SetRollup rollup = new SetRollup();
        rollup.computeFromSimpleMetrics(input);
        return rollup;
    }
    
    public static SetRollup buildRollupFromSetRollups(Points<SetRollup> input) throws IOException {
        SetRollup rollup = new SetRollup();
        rollup.computeFromRollups(BasicRollup.recast(input, IBasicRollup.class));
        return rollup;
    }
    
    public static SetRollup fromBasicRollup(IBasicRollup basic) {
        SetRollup rollup = new SetRollup();
        rollup.setCount(basic.getCount());
        rollup.setAverage((Average)basic.getAverage());
        rollup.setMin((MinValue)basic.getMinValue());
        rollup.setMax((MaxValue)basic.getMaxValue());
        rollup.setVariance((Variance)basic.getVariance());
        return rollup;
    }
}
