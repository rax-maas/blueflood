package com.cloudkick.blueflood.inputs.formats;

import com.cloudkick.blueflood.io.CqlTestBase;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.ServerMetricLocator;
import com.cloudkick.blueflood.utils.Util;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.Telescope;

import java.util.List;
import java.util.Map;

public class CloudMonitoringTelescopeTest {

    @Test
    public void testToMetrics() {
        Telescope tel = CqlTestBase.makeTelescope("1", "chFoo", "acBar", "module", "enId",
                "target", System.currentTimeMillis(), null);

        CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(tel);
        List<Metric> bfMetrics = cmTelescope.toMetrics();

        int count = 0;
        for (Map.Entry<String, telescope.thrift.Metric> entry : tel.getMetrics().entrySet()){
            Metric bfMetric = bfMetrics.get(count++);
            Locator locator = ServerMetricLocator.createFromTelescopePrimitives(tel.getAcctId(),
                    tel.getEntityId(), tel.getCheckId(),  Util.generateMetricName(entry.getKey(), tel.getMonitoringZoneId()));
            Assert.assertTrue(bfMetric.getLocator().equals(locator));
            Assert.assertEquals(tel.getTimestamp(), bfMetric.getCollectionTime());
            Assert.assertEquals(CloudMonitoringTelescope.getMetricUnitString(entry.getValue()), bfMetric.getUnit());
        }
    }
}
