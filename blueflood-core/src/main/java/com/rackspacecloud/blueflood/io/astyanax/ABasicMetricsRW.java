package com.rackspacecloud.blueflood.io.astyanax;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This class deals with reading/writing metrics to the metrics_{granularity} & metrics_string column families
 * using Astyanax driver
 */
public class ABasicMetricsRW extends AbstractMetricsRW {

    public ABasicMetricsRW(boolean isRecordingDelayedMetrics, Clock clock) {
        this.isRecordingDelayedMetrics = isRecordingDelayedMetrics;
        this.clock = clock;
    }

    @Override
    public void insertMetrics( Collection<IMetric> metrics ) throws IOException {

        try {
            AstyanaxWriter.getInstance().insertFull(metrics, isRecordingDelayedMetrics, clock);
        } catch (ConnectionException e) {
            throw new IOException(e);
        }
    }

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} rolled up
     * objects to the appropriate Cassandra column family
     *
     * @throws IOException
     */
    @Override
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws IOException {

        try {
            AstyanaxWriter.getInstance().insertRollups(writeContexts);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locator
     * @param range
     * @param gran
     * @return
     */
    @Override
    public MetricData getDatapointsForRange(Locator locator, Range range, Granularity gran) {

        return AstyanaxReader.getInstance().getDatapointsForRange( locator, range, gran );
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified list of {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locators
     * @param range
     * @param gran
     * @return
     */
    @Override
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators, Range range, Granularity gran) {

        return AstyanaxReader.getInstance().getDatapointsForRange( locators, range, gran );
    }

    /**
     * Fetches a {@link com.rackspacecloud.blueflood.types.Points} object for a
     * particular locator and rollupType from the specified column family and
     * range
     *
     * @param locator
     * @param rollupType
     * @param range
     * @param columnFamilyName

     * @return
     */
    @Override
    public Points<BasicRollup> getDataToRollup(final Locator locator,
                                               RollupType rollupType,
                                               Range range,
                                               String columnFamilyName) throws IOException {

        return AstyanaxReader.getInstance().getDataToRoll( BasicRollup.class,
                locator,
                range,
                CassandraModel.getColumnFamily( columnFamilyName ) );
    }
}
