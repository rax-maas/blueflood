package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.cm.Util;
import com.rackspacecloud.blueflood.types.ServerMetricLocator;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.Metric;
import telescope.thrift.Telescope;
import telescope.thrift.VerificationModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CloudMonitoringTelescopeTest {
    private static final char[] STRING_SEEDS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_".toCharArray();
    private static final Random rand = new Random(System.currentTimeMillis());

    @Test
    public void testToMetrics() {
        Telescope tel = makeTelescope("1", "chFoo", "acBar", "module", "enId",
                "target", System.currentTimeMillis(), null);

        CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(tel);
        List<com.rackspacecloud.blueflood.types.Metric> bfMetrics = cmTelescope.toMetrics();

        int count = 0;
        for (Map.Entry<String, telescope.thrift.Metric> entry : tel.getMetrics().entrySet()){
            com.rackspacecloud.blueflood.types.Metric bfMetric = bfMetrics.get(count++);
            ServerMetricLocator locator = ServerMetricLocator.createFromTelescopePrimitives(tel.getAcctId(),
                    tel.getEntityId(), tel.getCheckId(),  Util.generateMetricName(entry.getKey(), tel.getMonitoringZoneId()));
            Assert.assertTrue(bfMetric.getLocator().equals(locator));
            Assert.assertEquals(tel.getTimestamp(), bfMetric.getCollectionTime());
            Assert.assertEquals(CloudMonitoringTelescope.getMetricUnitString(entry.getValue()), bfMetric.getUnit());
        }
    }

    private Map<String, telescope.thrift.Metric> makeRandomIntMetrics(String dimension, int count) {
        Map<String, Metric> map = new HashMap<String, Metric>();
        for (int i = 0; i < count; i++) {
            Metric m = new Metric((byte)'i');
            m.setValueI32(rand.nextInt());
            map.put(dimension + "." + randString(8), m);
        }
        return map;
    }

    private String randString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(STRING_SEEDS[rand.nextInt(STRING_SEEDS.length)]);
        return sb.toString();
    }

    public static Map<String, Metric> makeMetrics(String dimension) {
        Map<String, Metric> metrics = new HashMap<String, Metric>();

        Metric intmetric = new Metric((byte)'i');
        intmetric.setValueI32(32 + Math.abs(rand.nextInt(10)));
        if (dimension != null)
            metrics.put(dimension + ".intmetric", intmetric);
        else
            metrics.put("intmetric", intmetric);

        return metrics;
    }

    public static Telescope makeTelescope(String id, String checkId, String acctId, String checkModule,
                                    String entityId, String target, long timestamp, String dimension) {
        Telescope tel = new Telescope(id, checkId, acctId, checkModule, entityId, target,
                timestamp, 1, VerificationModel.ONE);
        tel.setMetrics(makeMetrics(dimension));

        return tel;
    }
}
