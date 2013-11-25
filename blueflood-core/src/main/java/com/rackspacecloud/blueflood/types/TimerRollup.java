package com.rackspacecloud.blueflood.types;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TimerRollup extends BasicRollup {
    private long sum = 0;
    private double count_ps = 0;
    
    // to support percentiles, we will overload the count and treat it as sum.
    private Map<String, BasicRollup> percentiles = new HashMap<String, BasicRollup>();
    
    public TimerRollup() {
        
    }
    public TimerRollup(long sum, double count_ps, Number average, double variance, Number min, Number max, long count) {
        this();
        this.sum = sum;
        this.count_ps = count_ps;
        AbstractRollupStat.set(this.getAverage(), average);
        AbstractRollupStat.set(this.getVariance(), variance);
        AbstractRollupStat.set(this.getMinValue(), min);
        AbstractRollupStat.set(this.getMaxValue(), max);
        this.setCount(count);
    }
    
    public void setPercentile(String label, double mean, long sum, double upper, double lower) {
        BasicRollup br = new BasicRollup();
        AbstractRollupStat.set(br.getAverage(), mean);
        br.setCount(sum);
        AbstractRollupStat.set(br.getMaxValue(), upper);
        AbstractRollupStat.set(br.getMinValue(), lower);
        percentiles.put(label, br);
    }
    
    public double getCountPS() { return count_ps; }
    
    public String toString() {
        return String.format("%s, sum:%s, count_ps:%s, %s", super.toString(), sum, count_ps, 
                             Joiner.on(", ").withKeyValueSeparator(": ").join(percentiles.entrySet()));
    }
    
    private static double safeDiv(double numerator, double denominator) {
        if (denominator == 0)
            return 0d;
        else
            return numerator / denominator;
    }
    
    private void computeFromRollups(Points<TimerRollup> input) throws IOException {
        if (input == null)
            throw new IOException("Null input to create rollup from");
        if (input.isEmpty())
            return;
        
        Map<Long, Points.Point<TimerRollup>> points = input.getPoints();
        
        for (Map.Entry<Long, Points.Point<TimerRollup>> item : points.entrySet()) {
            TimerRollup rollup = item.getValue().getData();
            
            // todo: put this calculation in a static method and write tests for it.
            long count = this.getCount() + rollup.getCount();
            double time = safeDiv((double)getCount(), this.count_ps) + safeDiv((double) rollup.getCount(), rollup.count_ps);
            this.count_ps = safeDiv((double)count, time);
            
            // update fields.
            this.setCount(this.getCount() + rollup.getCount());
            this.sum += rollup.sum;
            
            this.getAverage().handleRollupMetric(rollup);
            this.getVariance().handleRollupMetric(rollup);
            this.getMinValue().handleRollupMetric(rollup);
            this.getMaxValue().handleRollupMetric(rollup);
            
            // now the percentiles.
            Map<String, BasicRollup> percentilesToMerge = rollup.getPercentiles();
            for (String label : percentilesToMerge.keySet()) {
                BasicRollup percentile = this.percentiles.get(label);
                BasicRollup toMerge = percentilesToMerge.get(label);
                if (percentile == null) {
                    percentile = new BasicRollup();
                    this.percentiles.put(label, percentile);
                }
                
                percentile.setCount(percentile.getCount() + toMerge.getCount());
                percentile.getAverage().handleRollupMetric(toMerge);
                percentile.getMaxValue().handleRollupMetric(toMerge);
                percentile.getMinValue().handleRollupMetric(toMerge);
            }
            
        }
    }
    
    
    public static double calculatePerSecond(long countA, double countPerSecA, long countB, double countPerSecB) {
        double totalCount = countA + countB;
        double totalTime = ((double)countA / countPerSecA) + ((double)countB / countPerSecB);
        return totalCount / totalTime;
    }
    
    public Map<String, BasicRollup> getPercentiles() {
        return Collections.unmodifiableMap(percentiles);
    }
    
    public static TimerRollup buildRollupFromRollups(Points<TimerRollup> input) throws IOException {
        TimerRollup rollup = new TimerRollup();
        rollup.computeFromRollups(input);
        return rollup;
    }
}
