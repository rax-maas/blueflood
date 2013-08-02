package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.Resolution;
import telescope.thrift.RollupMetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThriftRollupHandlerIntegrationTest extends IntegrationTestBase {
    final long baseMillis = 1335820166000L;
    final String acctId = "ac" + IntegrationTestBase.randString(8);
    final String metricName = "met_" + IntegrationTestBase.randString(8);
    final Locator[] locators = new Locator[] {
            Locator.createLocatorFromPathComponents(acctId, metricName)
    };

    @Override
    public void setUp() throws Exception {
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

    @Test
    public void testGetRollupByPoints() throws Exception {

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators)
                generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        final ThriftRollupHandler rh = new ThriftRollupHandler();

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
                Assert.assertEquals((int) answers.get(g2.name()), rms.size());
            }
        }
    }

    @Test
    public void testGetRollupByResolution() throws Exception {

        // generate every level of rollup for the raw data
        Granularity g = Granularity.FULL;
        while (g != Granularity.MIN_1440) {
            g = g.coarser();
            for (Locator locator : locators)
                generateRollups(locator, baseMillis, baseMillis + 86400000, g);
        }

        final ThriftRollupHandler rh = new ThriftRollupHandler();

        for (Locator locator : locators) {
            Assert.assertEquals(1440, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.FULL).getMetrics().size());
            Assert.assertEquals(289, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN5).getMetrics().size());
            Assert.assertEquals(73, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN20).getMetrics().size());
            Assert.assertEquals(25, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN60).getMetrics().size());
            Assert.assertEquals(7, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN240).getMetrics().size());
            Assert.assertEquals(2, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN1440).getMetrics().size());
        }
    }

    @Test
    public void testGetMissingRollupsByResolution() throws Exception {
        final ThriftRollupHandler rh = new ThriftRollupHandler();

        for (Locator locator : locators) {
            Assert.assertEquals(1440, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.FULL).getMetrics().size());
            Assert.assertEquals(288, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN5).getMetrics().size());
            Assert.assertEquals(72, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN20).getMetrics().size());
            Assert.assertEquals(24, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN60).getMetrics().size());
            Assert.assertEquals(6, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN240).getMetrics().size());
            Assert.assertEquals(1, rh.GetDataByResolution(locator.getAccountId(), locator.getMetricName(), baseMillis, baseMillis + 86400000, Resolution.MIN1440).getMetrics().size());
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
