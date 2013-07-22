package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.io.CqlTestBase;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.*;
import com.cloudkick.blueflood.utils.Util;
import junit.framework.Assert;
import telescope.thrift.MetricInfo;
import telescope.thrift.Resolution;
import telescope.thrift.RollupMetric;
import telescope.thrift.Telescope;

import java.util.*;

public class RollupHandlerIntegrationTest extends CqlTestBase {
    final long baseMillis = 1335820166000L;
    final String agentCheckName = "test_rollup_handler_agent";
    final String externalCheckName = "test_rollup_handler_external";
    final String dimension = "dim0";
    final String acctId = "ac" + CqlTestBase.randString(8);
    final String agentEntityId = "en" + CqlTestBase.randString(8);
    final String externalEntityId = "en" + CqlTestBase.randString(8);
    final String acctId2 = "otherAc" + CqlTestBase.randString(8);
    final String mzId = "mz" + CqlTestBase.randString(3).toUpperCase();
    final Locator[] locators = new ServerMetricLocator[] {
        ServerMetricLocator.createFromTelescopePrimitives(acctId, agentEntityId, agentCheckName,
                dimension + ".intmetric"),
        ServerMetricLocator.createFromTelescopePrimitives(acctId, externalEntityId, externalCheckName,
                Util.generateMetricName("intmetric", mzId)),
    };
    final Map<String, Set<String>> checkToMetricsMap = new HashMap<String, Set<String>>();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        // insert something every 1m for 24h
        for (int i = 0; i < 1440; i++) {
            final long curMillis = baseMillis + i * 60000;

            final List<Metric> metrics = new ArrayList<Metric>();
            Telescope tel = makeTelescope("uuid", agentCheckName, acctId, "module", agentEntityId, "target", curMillis, dimension);
            CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(tel);
            metrics.addAll(cmTelescope.toMetrics());

            tel = makeTelescope("uuid", externalCheckName, acctId, "module", externalEntityId, "target", curMillis, null);
            tel.setMonitoringZoneId(mzId);
            cmTelescope = new CloudMonitoringTelescope(tel);
            metrics.addAll(cmTelescope.toMetrics());

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
            locator = locators[0];// fix

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

    public void testGetMetricsForCheck() throws Exception {
        final RollupHandler rh = new RollupHandler();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        writer.insertFull(generateMetricsForTest());

        // test agent metrics list
        List<MetricInfo> metricsList  = rh.GetMetricsForCheck(acctId2, agentEntityId, agentCheckName);
        Set<String> expectedMetricNames = checkToMetricsMap.get(Util.generateMetricsDiscoveryDBKey(acctId2, agentEntityId,
                agentCheckName));
        String temp = null;
        for (MetricInfo metric : metricsList) {
            Assert.assertTrue(expectedMetricNames.contains(metric.getName()));
            Assert.assertTrue(metric.getUnit().equals("unknown"));
            if (temp == null) {
                temp = metric.getName();
            } else { // checks if the metric names are in lexical order
                Assert.assertTrue(metric.getName().compareTo(temp) >= 0);
                temp = metric.getName();
            }
        }

        temp = null;
        // test noit metrics list
        metricsList  = rh.GetMetricsForCheck(acctId2, externalEntityId, externalCheckName);
        expectedMetricNames = checkToMetricsMap.get(Util.generateMetricsDiscoveryDBKey(acctId2, externalEntityId,
                externalCheckName));
        for (MetricInfo metric : metricsList) {
            Assert.assertTrue(expectedMetricNames.contains(metric.getName()));
            if (temp == null) {
                temp = metric.getName();
            } else { // checks if the metric names are in lexical order
                Assert.assertTrue(metric.getName().compareTo(temp) >= 0);
                temp = metric.getName();
            }
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

    private List<Metric> generateMetricsForTest() {
        final List<Metric> metrics = new ArrayList<Metric>();
        final List<Telescope> telescopes = generateTelescopes();

        for (final Telescope tel : telescopes) {
            CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(tel);
            metrics.addAll(cmTelescope.toMetrics());
        }

        return metrics;
    }

    private List<Telescope> generateTelescopes() {
        int TOTAL_TELESCOPES = 20;
        long curSecs = 12345L;
        List<Telescope> telescopes = new ArrayList<Telescope>();

        Set<String> agentMetrics = new HashSet<String>();
        Set<String> noitMetrics = new HashSet<String>();

        for (int i = 0; i < TOTAL_TELESCOPES; i++) {
            String dimension =  "dim" +  CqlTestBase.randString(1);
            String mzId = "mz" + CqlTestBase.randString(3).toUpperCase();
            // agent metric with dimension
            telescopes.add(makeTelescope("uuid", agentCheckName, acctId2, "module", agentEntityId, "target",
                    curSecs * 1000,
                    dimension));
            agentMetrics.add(dimension + ".intmetric");

            // noit metric with monitoring zone
            Telescope withMz = makeTelescope("uuid", externalCheckName, acctId2, "module", externalEntityId, "target",
                    curSecs * 1000, null);
            withMz.setMonitoringZoneId(mzId);
            telescopes.add(withMz);
            noitMetrics.add(mzId + ".intmetric");
        }

        // agent metric without dimension
        telescopes.add(makeTelescope("uuid", agentCheckName, acctId2, "module", agentEntityId, "target",
                curSecs * 1000,
                null));
        agentMetrics.add("intmetric");

        checkToMetricsMap.put(Util.generateMetricsDiscoveryDBKey(acctId2, agentEntityId, agentCheckName), agentMetrics);
        checkToMetricsMap.put(Util.generateMetricsDiscoveryDBKey(acctId2, externalEntityId, externalCheckName), noitMetrics);

        return telescopes;
    }
}
