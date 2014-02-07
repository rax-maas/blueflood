package com.rackspacecloud.blueflood.types;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.rackspacecloud.blueflood.utils.Util;

import java.io.IOException;
import java.util.*;


public class TimerRollup implements Rollup, IBasicRollup {
    private long sum = 0;
    private long count = 0;
    private double rate = 0;

    /**
     * Number of pre-aggregated timers received by Blueflood
     * No relationship to 'count', which indicates number of raw timings.
     * If you have a 5-minute rollup and sent a Timer to Blueflood every 60 seconds,
     * the value would be 5.
     */
    private int sampleCount = 0;
    private AbstractRollupStat min = new MinValue();
    private AbstractRollupStat max = new MaxValue();
    private AbstractRollupStat average = new Average();
    private AbstractRollupStat variance = new Variance();
    
    // to support percentiles, we will overload the count and treat it as sum.
    private Map<String, Percentile> percentiles = new HashMap<String, Percentile>();
    
    public TimerRollup() {
        super();
    }
    
    public TimerRollup withSum(long sum) {
        this.sum = sum;
        return this;
    }

    public TimerRollup withCount(long count) {
        this.count = count;
        return this;
    }

    public TimerRollup withCountPS(double count_ps) {
        this.rate = count_ps;
        return this;
    }

    public TimerRollup withSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
        return this;
    }
    
    public TimerRollup withMinValue(AbstractRollupStat min) {
        this.min = min;
        return this;
    }
    
    public TimerRollup withMinValue(Number num) {
        AbstractRollupStat.set(this.min, num);
        return this;
    }
    
    public TimerRollup withMaxValue(AbstractRollupStat max) {
        this.max = max;
        return this;
    }
    
    public TimerRollup withMaxValue(Number num) {
        AbstractRollupStat.set(this.max, num);
        return this;
    }
    
    public TimerRollup withAverage(AbstractRollupStat average) {
        this.average = average;
        return this;
    }
    
    public TimerRollup withAverage(Number average) {
        AbstractRollupStat.set(this.average, average);
        return this;
    }
    
    public TimerRollup withVariance(AbstractRollupStat variance) {
        this.variance = variance;
        return this;
    }
    
    public TimerRollup withVariance(Number variance) {
        AbstractRollupStat.set(this.variance, variance);
        return this;
    }
    
    public AbstractRollupStat getAverage() { return average; }
    public AbstractRollupStat getMaxValue() { return max; }
    public AbstractRollupStat getMinValue() { return min; }
    public AbstractRollupStat getVariance() { return variance; }
    
    public void setPercentile(String label, Number mean) {
        percentiles.put(label, new Percentile(mean));
    }

    @Override
    public Boolean hasData() {
        return sampleCount > 0;
    }

    // todo: consider moving this to its own class.
    public static class Percentile {
        private Number mean;
        
        public Percentile(Number mean) {
            // longs and doubles only please.
            this.mean = maybePromote(mean);
        }
        
        public boolean equals(Object obj) {
            if (!(obj instanceof Percentile)) return false;
            Percentile other = (Percentile)obj;
            if (!other.mean.equals(this.mean)) return false;
            return true;
        }
        
        public Number getMean() { return mean; }
        
        public String toString() {
            return String.format("{mean:%s}", mean.toString());
        }
        
        public static Number maybePromote(Number number) {
            if (number instanceof Float)
                return number.doubleValue();
            else if (number instanceof Integer)
                return number.longValue();
            else
                return number;
        }
        
    }
    
    // per second rate.
    public double getRate() { return rate; }
    public long getSum() { return sum; }
    public long getCount() { return count; };
    public int getSampleCount() { return sampleCount; }
    
    public String toString() {
        return String.format("sum:%s, rate:%s, count:%s, min:%s, max:%s, avg:%s, var:%s, sample_cnt:%s, %s",
                sum, rate, count, min, max, average, variance, sampleCount,
                Joiner.on(", ").withKeyValueSeparator(": ").join(percentiles.entrySet()));
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.TIMER;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof TimerRollup)) return false;
        TimerRollup other = (TimerRollup)obj;

        if (other.sum != this.sum) return false;
        if (other.sampleCount != this.sampleCount) return false;
        if (other.rate != this.rate) return false;
        if (!other.average.equals(this.average)) return false;
        if (!other.variance.equals(this.variance)) return false;
        if (!other.min.equals(this.min)) return false;
        if (!other.max.equals(this.max)) return false;
        if (other.count != this.count) return false;

        
        Map<String, Percentile> otherPct = other.getPercentiles();
        Set<String> allKeys = Sets.union(otherPct.keySet(), this.getPercentiles().keySet());
        if (allKeys.size() != this.getPercentiles().size()) return false;
        
        for (Map.Entry<String, Percentile> otherEntry : otherPct.entrySet())
            if (!otherEntry.getValue().equals(this.getPercentiles().get(otherEntry.getKey())))
                return false;
        return true;
    }
    
    private void computeFromRollups(Points<TimerRollup> input) throws IOException {
        if (input == null)
            throw new IOException("Null input to create rollup from");
        if (input.isEmpty())
            return;
        
        Map<Long, Points.Point<TimerRollup>> points = input.getPoints();
        Set<String> labels = new HashSet<String>();
        Multimap<String, Number> pctMeans = LinkedListMultimap.create();
        Multimap<String, Number> pctUppers = LinkedListMultimap.create();
        Multimap<String, Number> pctSums = LinkedListMultimap.create();

        for (Map.Entry<Long, Points.Point<TimerRollup>> item : points.entrySet()) {
            TimerRollup rollup = item.getValue().getData();
            
            // todo: put this calculation in a static method and write tests for it.
            long count = this.getCount() + rollup.getCount();
            double time = Util.safeDiv((double) getCount(), this.rate) + Util.safeDiv((double) rollup.getCount(), rollup.rate);
            this.rate = Util.safeDiv((double) count, time);
            
            // update fields.
            this.count += rollup.getCount();
            this.sum += rollup.getSum();
            this.sampleCount += rollup.getSampleCount();
            
            this.average.handleRollupMetric(rollup);
            this.variance.handleRollupMetric(rollup);
            this.min.handleRollupMetric(rollup);
            this.max.handleRollupMetric(rollup);
            
            // now the percentiles.
            Map<String, Percentile> percentilesToMerge = rollup.getPercentiles();
            for (String label : percentilesToMerge.keySet()) {
                labels.add(label);
                Percentile percentile = percentilesToMerge.get(label);
                pctMeans.get(label).add(percentile.getMean());
            }
        }
        
        // now go through the percentiles and calculate!
        for (String label : labels) {
            Number mean = TimerRollup.avg(pctMeans.get(label));
            this.setPercentile(label, mean);
        }
        // wooo!
    }
    
    public static Number sum(Collection<Number> numbers) {
        long longSum = 0;
        double doubleSum = 0d;
        boolean useDouble = false;
        
        for (Number number : numbers) {
            if (useDouble || number instanceof Double || number instanceof Float) {
                if (!useDouble) {
                    useDouble = true;
                    doubleSum += longSum;
                }
                doubleSum += number.doubleValue();
            } else if (number instanceof Long || number instanceof Integer)
                longSum += number.longValue();
        }
        
        if (useDouble)
            return doubleSum;
        else
            return longSum;
    }
    
    public static Number avg(Collection<Number> numbers) {
        Number sum = TimerRollup.sum(numbers);
        if (sum instanceof Long || sum instanceof Integer)
            return (Long)sum / numbers.size();
        else
            return (Double)sum / (double)numbers.size();
    }
    
    public static Number max(Collection<Number> numbers) {
        long longMax = numbers.iterator().next().longValue();
        double doubleMax = numbers.iterator().next().doubleValue();
        boolean useDouble = false;
        
        for (Number number : numbers) {
            if (useDouble || number instanceof Double || number instanceof Float) {
                if (!useDouble) {
                    useDouble = true;
                    doubleMax = Math.max(doubleMax, (double)longMax);
                }
                doubleMax = Math.max(doubleMax, number.doubleValue());
            } else {
                longMax = Math.max(longMax, number.longValue());
            }
        }
        
        if (useDouble)
            return doubleMax;
        else
            return longMax;
    }
    
    public static double calculatePerSecond(long countA, double countPerSecA, long countB, double countPerSecB) {
        double totalCount = countA + countB;
        double totalTime = ((double)countA / countPerSecA) + ((double)countB / countPerSecB);
        return totalCount / totalTime;
    }
    
    public Map<String, Percentile> getPercentiles() {
        return Collections.unmodifiableMap(percentiles);
    }
    
    public static TimerRollup buildRollupFromTimerRollups(Points<TimerRollup> input) throws IOException {
        TimerRollup rollup = new TimerRollup();
        rollup.computeFromRollups(input);
        return rollup;
    }
}
