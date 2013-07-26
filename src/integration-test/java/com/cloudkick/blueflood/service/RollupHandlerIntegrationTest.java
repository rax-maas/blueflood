package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.io.IntegrationTestBase;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.Range;
import com.cloudkick.blueflood.types.Rollup;
import telescope.thrift.Resolution;
import telescope.thrift.RollupMetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RollupHandlerIntegrationTest extends IntegrationTestBase {
    final long baseMillis = 1335820166000L;
    final String acctId = "ac" + IntegrationTestBase.randString(8);
    final String metricName = "met_" + IntegrationTestBase.randString(8);
    final Locator[] locators = new Locator[] {
            Locator.createLocatorFromPathComponents(acctId, metricName)
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();

        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;
            final List<Metric> metrics = new ArrayList<Metric>();
            final Metric metric = getRandomIntMetric(locators[0], curMillis);
            metrics.add(metric);

            writer.insertFull(metrics);
        }
    }

    public void testGetRollupByPoints() throws Exception {

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators)
                generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        final RollupHandler rh = new RollupHandler();

        final Map<String, Integer> answers = new HashMap<String, Integer>();
        answers.put("all of them", 1440);
        answers.put(Granularity.FULL.name(), 289);
        answers.put(Granularity.MIN_5.name(), 289);
        answers.put(Granularity.MIN_20.name(), 73);
        answers.put(Granularity.MIN_60.name(), 25);
        answers.put(Granularity.MIN_240.name(), 7);
        answers.put(Granularity.MIN_1440.name(), 2);

        final Map<String, Integer> points = new HashMap<String, Integer>();
        points.put("all of them", 1700);
        points.put(Granularity.FULL.name(), 800);
        points.put(Granularity.MIN_5.name(), 287);
        points.put(Granularity.MIN_20.name(), 71);
        points.put(Granularity.MIN_60.name(), 23);
        points.put(Granularity.MIN_240.name(), 5);
        points.put(Granularity.MIN_1440.name(), 1);

        for (Locator locator : locators) {

            for (Granularity g2 : Granularity.granularities()) {
                List<RollupMetric> rms = rh.GetDataByPoints(
                        locator.getAccountId(),
                        locator.getMetricName(),
                        baseMillis,
                        baseMillis + 86400000,
                        points.get(g2.name())).getMetrics();
                assertEquals((int) answers.get(g2.name()), rms.size());
            }
        }
    }

    public void testGetRollupByResolution() throws Exception {

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators)
                generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        final RollupHandler rh = new RollupHandler();

        for (Locator locator : locators) {
            assertEquals(1440, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.FULL).getMetrics().size());
            assertEquals(289, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN5).getMetrics().size());
            assertEquals(73, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN20).getMetrics().size());
            assertEquals(25, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN60).getMetrics().size());
            assertEquals(7, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN240).getMetrics().size());
            assertEquals(2, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN1440).getMetrics().size());
        }
    }

    public void testGetMissingRollupsByResolution() throws Exception {
        final RollupHandler rh = new RollupHandler();

        for (Locator locator : locators) {
            assertEquals(1440, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.FULL).getMetrics().size());
            assertEquals(288, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN5).getMetrics().size());
            assertEquals(72, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN20).getMetrics().size());
            assertEquals(24, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN60).getMetrics().size());
            assertEquals(6, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN240).getMetrics().size());
            assertEquals(1, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN1440).getMetrics().size());
        }
    }


    private void generateRollups(Locator locator, long from, long to, Granularity destGranularity) throws Exception {
        if (destGranularity == Granularity.FULL) {
            throw new Exception("Can't roll up to FULL");
        }

        Map<Long, Rollup> rollups = new HashMap<Long, Rollup>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            Rollup rollup = AstyanaxReader.getInstance().readAndCalculate(locator, range, Granularity.FULL);
            rollups.put(range.getStart(), rollup);
        }

        AstyanaxWriter.getInstance().insertRollups(locator, rollups, destGranularity);
    }
}
