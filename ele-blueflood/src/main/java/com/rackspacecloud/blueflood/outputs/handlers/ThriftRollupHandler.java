package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.DefaultThriftOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.OutputSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Resolution;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telescope.thrift.MetricInfo;
import telescope.thrift.RollupMetrics;
import telescope.thrift.RollupServer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ThriftRollupHandler extends RollupHandler
        implements RollupServer.Iface, MetricDataQueryInterface<RollupMetrics> {
    private static final Logger log = LoggerFactory.getLogger(ThriftRollupHandler.class);
    private final OutputSerializer<RollupMetrics> outputSerializer;
    private static final Set<String> defaultStats;

    static {
        defaultStats = new HashSet<String>();
        defaultStats.add("average");
        defaultStats.add("variance");
        defaultStats.add("min");
        defaultStats.add("max");
    }

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
        try {
            return GetRollups(accountId, metricName, from, to, g);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public RollupMetrics GetDataByResolution(
            String accountId,
            String metricName,
            long from,
            long to,
            telescope.thrift.Resolution resolution) throws TException {
        rollupsByGranularityMeter.mark();
        if (resolution == null) {
          throw new TException("Resolution is not set");
        }
        try {
            return GetDataByResolution(accountId, metricName, from, to, Resolution.valueOf(resolution.toString()));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public RollupMetrics GetDataByResolution(String accountId,
                                             String metric,
                                             long from,
                                             long to,
                                             Resolution resolution) throws SerializationException {
        Granularity g = Granularity.granularities()[resolution.getValue()];
        return GetRollups(accountId, metric, from, to, g);
    }

    private RollupMetrics GetRollups(
            String accountId,
            String metricName,
            long from,
            long to,
            Granularity g) throws SerializationException {
        MetricData metricData = getRollupByGranularity(accountId, metricName, from, to, g);

        return outputSerializer.transformRollupData(metricData, defaultStats);
    }

    @Override
    public List<MetricInfo> GetMetricsForCheck(String acctId, String entityId, String checkId) throws TException {
        throw new TException("Not implemented.");
    }
}
