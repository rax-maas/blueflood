package com.rackspacecloud.blueflood.outputs.handlers;

import com.netflix.astyanax.model.Column;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.RackIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.TException;
import telescope.thrift.MetricInfo;

import java.util.ArrayList;
import java.util.List;

// todo: CM_SPECIFIC
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
    
        final String dBKey = Util.generateMetricsDiscoveryDBKey(accountId, entityId, checkId);
        for (Column<String> col : reader.getMetricsList(dBKey)) {
            String metric = col.getName();
            String unitString = AstyanaxReader.getUnitString(Locator.createLocatorFromPathComponents(accountId, entityId, checkId, metric));
            results.add(new MetricInfo(col.getName(), unitString));
        }
        return results;
    }
}
