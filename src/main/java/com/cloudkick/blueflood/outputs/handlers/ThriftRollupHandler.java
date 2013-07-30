package com.cloudkick.blueflood.outputs.handlers;

import com.cloudkick.blueflood.outputs.Serializers.DefaultThriftOutputSerializer;
import com.cloudkick.blueflood.outputs.Serializers.OutputSerializer;
import com.cloudkick.blueflood.outputs.formats.RollupData;
import com.cloudkick.blueflood.rollup.Granularity;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope.thrift.MetricInfo;
import telescope.thrift.Resolution;
import telescope.thrift.RollupMetrics;
import telescope.thrift.RollupServer;

import java.util.List;

public class ThriftRollupHandler extends RollupHandler
        implements RollupServer.Iface, RollupsQueryInterface<RollupMetrics> {
    private static final Logger log = LoggerFactory.getLogger(ThriftRollupHandler.class);
    private final OutputSerializer<RollupMetrics> outputSerializer;

    public ThriftRollupHandler(OutputSerializer<RollupMetrics> outputSerializer) {
        this.outputSerializer = outputSerializer;
    }

    public ThriftRollupHandler() {
        this.outputSerializer = new DefaultThriftOutputSerializer();
    }

    @Override
    public RollupMetrics GetDataByPoints(
            String accountId,
            String metricName,
            long from,
            long to,
            int points) throws TException {
        rollupsByPointsMeter.mark();
        Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
        return GetRollups(accountId, metricName, from, to, g);
    }

    @Override
    public RollupMetrics GetDataByResolution(
            String accountId,
            String metricName,
            long from,
            long to,
            Resolution resolution) throws TException {
        rollupsByGranularityMeter.mark();
        if (resolution == null)
          throw new TException("Resolution is not set");
        Granularity g = Granularity.granularities()[resolution.getValue()];
        return GetRollups(accountId, metricName, from, to, g);
    }

    private RollupMetrics GetRollups(
            String accountId,
            String metricName,
            long from,
            long to,
            Granularity g) throws TException {
        RollupData rollupData = getRollupByGranularity(accountId, metricName, from, to, g);
        return outputSerializer.transformRollupData(rollupData);
    }

    @Override
    public List<MetricInfo> GetMetricsForCheck(String acctId, String entityId, String checkId) throws TException {
        throw new TException("Not implemented.");
    }
}
