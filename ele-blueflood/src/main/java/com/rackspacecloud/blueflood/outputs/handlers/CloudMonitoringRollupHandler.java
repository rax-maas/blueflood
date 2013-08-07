package com.rackspacecloud.blueflood.outputs.handlers;

import com.netflix.astyanax.model.Column;
import com.rackspacecloud.blueflood.CloudMonitoringUtils;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.RackIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.TException;
import telescope.thrift.MetricInfo;

import java.util.ArrayList;
import java.util.List;

public class CloudMonitoringRollupHandler extends ThriftRollupHandler {

    public List<MetricInfo> GetMetricsForCheck(String accountId, String entityId, String checkId) throws TException {
        final TimerContext ctx = metricsForCheckTimer.time();
        List<MetricInfo> metrics = CloudMonitoringRollupHandler.getMetricsForCheck(
                RackIO.getInstance(),
                accountId,
                entityId,
                checkId);
        ctx.stop();

        return metrics;
    }
    
    public static List<MetricInfo> getMetricsForCheck(RackIO reader, String accountId, String entityId, String checkId) {
        final List<MetricInfo> results = new ArrayList<MetricInfo>();
    
        // todo: relying on Astyanax internals is kind of leaky here.  This should get pushed down into RackIO.
        final String dBKey = CloudMonitoringUtils.generateMetricsDiscoveryDBKey(accountId, entityId, checkId);
        for (Column<String> col : reader.getMetricsList(dBKey)) {
            String metric = col.getName();
            String unitString = AstyanaxReader.getUnitString(Locator.createLocatorFromPathComponents(accountId, entityId, checkId, metric));
            results.add(new MetricInfo(col.getName(), unitString));
        }
        return results;
    }
}
