package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.TException;
import telescope.thrift.MetricInfo;

import java.util.List;

public class CloudMonitoringRollupHandler extends ThriftRollupHandler {

    public List<MetricInfo> GetMetricsForCheck(String accountId, String entityId, String checkId) throws TException {
        final TimerContext ctx = metricsForCheckTimer.time();
        List<MetricInfo> metrics = AstyanaxReader.getInstance().getMetricsForCheck(accountId, entityId, checkId);
        ctx.stop();

        return metrics;
    }
}
