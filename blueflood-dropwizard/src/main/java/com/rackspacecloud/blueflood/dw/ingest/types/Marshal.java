package com.rackspacecloud.blueflood.dw.ingest.types;

import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Marshal {
    private static final TimeValue DEFAULT_PREAG_TTL = new TimeValue(48, TimeUnit.HOURS);
    
    // tenantId can be null!
    public static Collection<Metric> remarshal(Collection<BasicMetric> basicMetrics, String tenantId) {
        List<Metric> metrics = new ArrayList<Metric>(basicMetrics.size());
        for (BasicMetric bm : basicMetrics) {
            String appliedTenantId = tenantId == null ? bm.getTenant() : tenantId;
            Locator locator = Locator.createLocatorFromPathComponents(appliedTenantId, splitForLocator(bm.getMetricName()));
            Metric m = new Metric(locator, bm.getMetricValue(), bm.getCollectionTime(), new TimeValue(bm.getTtlInSeconds(), TimeUnit.SECONDS), bm.getUnit());
            metrics.add(m);
        }
        return metrics;
    }
    
    // tenantId can be null!
    public static Collection<IMetric> remarshal(Bundle bundle, String tenantId) throws IOException {
        List<IMetric> metrics = new ArrayList<IMetric>();
        metrics.addAll(remarshalCounters(bundle, tenantId));
        metrics.addAll(remarshalGauges(bundle, tenantId)); // this is where the IOException can be thrown.
        metrics.addAll(remarshalSets(bundle, tenantId));
        metrics.addAll(remarshalTimers(bundle, tenantId));
        return metrics;
    }
    
    // tenantId can be null!
    private static Collection<PreaggregatedMetric> remarshalCounters(Bundle bundle, String tenantId) {
        final Collection<Counter> counters = bundle.getCounters();
        final List<PreaggregatedMetric> metrics = new ArrayList<PreaggregatedMetric>(counters.size());
        for (Counter c : counters) {
            Locator locator = Locator.createLocatorFromPathComponents(tenantId == null ? c.getTenant() : tenantId, splitForLocator(c.getName()));
            long sampleCount = bundle.getFlushInterval() > 0
                    ? (long)(c.getRate().doubleValue() * ((double)bundle.getFlushInterval()/1000d))
                    : 1;
            Rollup rollup = new CounterRollup()
                    .withCount(c.getValue())
                    .withRate(c.getRate().doubleValue())
                    .withCount(sampleCount);
            PreaggregatedMetric metric = new PreaggregatedMetric(bundle.getCollectionTime(), locator, DEFAULT_PREAG_TTL, rollup);
            metrics.add(metric);
        }
        return metrics;
    }
    
    // tenantId can be null!
    private static Collection<PreaggregatedMetric> remarshalGauges(Bundle bundle, String tenantId) throws IOException {
        final Collection<Gauge> gauges = bundle.getGauges();
        final List<PreaggregatedMetric> metrics = new ArrayList<PreaggregatedMetric>(gauges.size());
        for (Gauge g : gauges) {
            Locator locator = Locator.createLocatorFromPathComponents(tenantId == null ? g.getTenant() : tenantId, splitForLocator(g.getName()));
            Points<SimpleNumber> points = new Points<SimpleNumber>();
            points.add(new Points.Point<SimpleNumber>(bundle.getCollectionTime(), new SimpleNumber(g.getValue())));
            Rollup rollup = GaugeRollup.buildFromRawSamples(points);
            PreaggregatedMetric metric = new PreaggregatedMetric(bundle.getCollectionTime(), locator, DEFAULT_PREAG_TTL, rollup);
            metrics.add(metric);
        }
        return metrics;
    }
    
    // tenantId can be null!
    private static Collection<PreaggregatedMetric> remarshalSets(Bundle bundle, String tenantId) {
        final Collection<Set> sets = bundle.getSets();
        final List<PreaggregatedMetric> metrics = new ArrayList<PreaggregatedMetric>(sets.size());
        for (Set s : sets) {
            Locator locator = Locator.createLocatorFromPathComponents(tenantId == null ? s.getTenant() : tenantId, splitForLocator(s.getName()));
            SetRollup rollup = new SetRollup();
            for (String value : s.getValues()) {
                rollup = rollup.withObject(value);
            }
            PreaggregatedMetric metric = new PreaggregatedMetric(bundle.getCollectionTime(), locator, DEFAULT_PREAG_TTL, rollup);
            metrics.add(metric);
        }
        return metrics;
    }
    
    // tenantId can be null!
    private static Collection<PreaggregatedMetric> remarshalTimers(Bundle bundle, String tenantId) {
        final Collection<Timer> timers = bundle.getTimers();
        final List<PreaggregatedMetric> metrics = new ArrayList<PreaggregatedMetric>(timers.size());
        for (Timer t : timers) {
            Locator locator = Locator.createLocatorFromPathComponents(tenantId == null ? t.getTenant() : tenantId, splitForLocator(t.getName()));
            TimerRollup rollup = new TimerRollup()
                    .withCount(t.getCount().longValue())
                    .withSampleCount(1)
                    .withAverage(t.getAvg())
                    .withMaxValue(t.getMax())
                    .withMinValue(t.getMin())
                    .withCountPS(t.getRate().doubleValue())
                    .withSum(t.getSum().doubleValue())
                    .withVariance(Math.pow(t.getStd().doubleValue(), 2d));
            
            // percentiles.
            for (Map.Entry<String, Number> entry : t.getPercentiles().entrySet()) {
                rollup.setPercentile(entry.getKey(), entry.getValue());
            }
            
            // histograms are ignored.
            PreaggregatedMetric metric = new PreaggregatedMetric(bundle.getCollectionTime(), locator, DEFAULT_PREAG_TTL, rollup);
            metrics.add(metric);
        }
        return metrics;
    }
    
    private static String[] splitForLocator(String s) {
        return s.split("\\.", -1);
    }
}
