package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.cm.Util;
import com.rackspacecloud.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.rackspacecloud.blueflood.inputs.formats.CloudMonitoringTelescopeTest;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.RackIO;
import com.rackspacecloud.blueflood.outputs.handlers.CloudMonitoringRollupHandler;
import com.rackspacecloud.blueflood.types.Metric;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.MetricInfo;
import telescope.thrift.Telescope;

import java.util.*;

public class CloudMonitoringRollupHandlerIntegrationTest extends IntegrationTestBase {
    final String agentCheckName = "test_rollup_handler_agent";
    final String externalCheckName = "test_rollup_handler_external";
    final String agentEntityId = "en" + IntegrationTestBase.randString(8);
    final String externalEntityId = "en" + IntegrationTestBase.randString(8);
    final String acctId2 = "otherAc" + IntegrationTestBase.randString(8);
    final Map<String, Set<String>> checkToMetricsMap = new HashMap<String, Set<String>>();

    @Test
    public void testGetMetricsForCheck() throws Exception {
        final CloudMonitoringRollupHandler rh = new CloudMonitoringRollupHandler();
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        RackIO rackIO = RackIO.getInstance();
        List<Metric> metrics = generateMetricsForTest();
        writer.insertFull(metrics);
        rackIO.insertDiscovery(metrics);

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
            String dimension =  "dim" +  IntegrationTestBase.randString(1);
            String mzId = "mz" + IntegrationTestBase.randString(3).toUpperCase();
            // agent metric with dimension
            telescopes.add(CloudMonitoringTelescopeTest.makeTelescope("uuid", agentCheckName, acctId2, "module", agentEntityId, "target",
                    curSecs * 1000,
                    dimension));
            agentMetrics.add(dimension + ".intmetric");

            // noit metric with monitoring zone
            Telescope withMz = CloudMonitoringTelescopeTest.makeTelescope("uuid", externalCheckName, acctId2, "module", externalEntityId, "target",
                    curSecs * 1000, null);
            withMz.setMonitoringZoneId(mzId);
            telescopes.add(withMz);
            noitMetrics.add(mzId + ".intmetric");
        }

        // agent metric without dimension
        telescopes.add(CloudMonitoringTelescopeTest.makeTelescope("uuid", agentCheckName, acctId2, "module", agentEntityId, "target",
                curSecs * 1000,
                null));
        agentMetrics.add("intmetric");

        checkToMetricsMap.put(Util.generateMetricsDiscoveryDBKey(acctId2, agentEntityId, agentCheckName), agentMetrics);
        checkToMetricsMap.put(Util.generateMetricsDiscoveryDBKey(acctId2, externalEntityId, externalCheckName), noitMetrics);

        return telescopes;
    }
}
