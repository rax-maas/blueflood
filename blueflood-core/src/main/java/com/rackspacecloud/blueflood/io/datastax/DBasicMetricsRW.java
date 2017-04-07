package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the basic metrics_* column families
 * using Datastax driver
 */
public class DBasicMetricsRW extends DAbstractMetricsRW {

    private static final Logger LOG = LoggerFactory.getLogger(DBasicMetricsRW.class);

    private DSimpleNumberIO simpleNumberIO = new DSimpleNumberIO();
    private final DBasicNumericIO basicIO = new DBasicNumericIO();

    /**
     * Constructor
     * @param isRecordingDelayedMetrics if true, delayed metrics are recorded in metrics_delayed_locator
     * @param isBatchIngestEnabled  if true, metrics are inserted using batch statement
     */
    @VisibleForTesting
    public DBasicMetricsRW(DLocatorIO locatorIO, DDelayedLocatorIO delayedLocatorIO,
                           boolean isRecordingDelayedMetrics,
                           boolean isBatchIngestEnabled, Clock clock) {
        super(locatorIO, delayedLocatorIO, isRecordingDelayedMetrics, isBatchIngestEnabled, clock);
    }

    /**
     * Constructor. Using this, batch Ingest is NOT enabled.
     * @param isRecordingDelayedMetrics
     */
    @VisibleForTesting
    public DBasicMetricsRW(DLocatorIO locatorIO, DDelayedLocatorIO delayedLocatorIO,
                           boolean isRecordingDelayedMetrics,
                           Clock clock) {
        super(locatorIO, delayedLocatorIO, isRecordingDelayedMetrics, false, clock);
    }

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} objects
     * to the appropriate Cassandra column family. Effectively, this method is only called to write
     * the raw metrics received during Ingest requests. Another method, insertRollups, is used by
     * the Rollup processes to write rolled up metrics.
     *
     * @param metrics
     *
     * @throws IOException
     */
    @Override
    public void insertMetrics( Collection<IMetric> metrics ) throws IOException {

        Timer.Context ctx = Instrumentation.getWriteTimerContext( CassandraModel.CF_METRICS_FULL_NAME );
        try {
            if ( isBatchIngestEnabled ) {
                insertMetricsInBatch(metrics);
            } else {
                insertMetricsIndividually(metrics);
            }
        }
        finally {
            ctx.stop();
        }
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
    public Map<Locator, MetricData> getDatapointsForRange( List<Locator> locators, Range range, Granularity gran ) {

        Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();

        String columnFamily = CassandraModel.getBasicColumnFamilyName( gran );

        metrics.putAll( super.getDatapointsForRange( locators, range, columnFamily, gran));

        return metrics;
    }

    /**
     * Return the appropriate IO object which interacts with the Cassandra database.
     *
     * For Preaggregated Metrics, only rollup type is required.
     * For Basic Numeric Metrics, the granularity is required as metrics_full is handled differently
     * than the other granularities.
     *
     * @param rollupType
     * @param granularity
     * @return
     */
    @Override
    public DAbstractMetricIO getIO( String rollupType, Granularity granularity ) {

      if( granularity == Granularity.FULL )
        return simpleNumberIO;
      else
        return basicIO;
    }

    private void insertMetricsIndividually(Collection<IMetric> metrics) throws IOException {

        Map<Locator, ResultSetFuture> futures = new HashMap<Locator, ResultSetFuture>();
        for( IMetric metric : metrics ) {

            Locator locator = metric.getLocator();

            if( !LocatorCache.getInstance().isLocatorCurrentInBatchLayer(locator) ) {

                LocatorCache.getInstance().setLocatorCurrentInBatchLayer(locator);
                locatorIO.insertLocator( locator );
            }

            if (isRecordingDelayedMetrics) {
                insertLocatorIfDelayed(metric);
            }

            futures.put(locator, simpleNumberIO.insertRawAsync(metric));

            Instrumentation.markFullResMetricWritten();
        }

        for( Map.Entry<Locator, ResultSetFuture> f : futures.entrySet() ) {

            try {
                ResultSet result = f.getValue().getUninterruptibly();

                LOG.trace( "result.size=" + result.all().size() );
            }
            catch ( Exception e ) {
                Instrumentation.markWriteError();
                LOG.error(String.format("error writing metric for locator %s",
                        f.getKey()), e );
            }
        }
    }

    /**
     * Inserts a collection of metrics in a batch using an unlogged
     * {@link BatchStatement}
     *
     * @param metrics
     * @return
     */
    private void insertMetricsInBatch(Collection<IMetric> metrics) throws IOException {

        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

        for (IMetric metric : metrics) {
            BoundStatement bound = simpleNumberIO.getBoundStatementForMetric(metric);
            batch.add(bound);
            Instrumentation.markFullResMetricWritten();

            Locator locator = metric.getLocator();
            if( !LocatorCache.getInstance().isLocatorCurrentInBatchLayer(locator) ) {
                LocatorCache.getInstance().setLocatorCurrentInBatchLayer(locator);
                batch.add(locatorIO.getBoundStatementForLocator( locator ));
            }

            // if we are recording delayed metrics, we may need to do an
            // extra insert
            if ( isRecordingDelayedMetrics ) {
                BoundStatement bs = getBoundStatementForMetricIfDelayed(metric);
                if ( bs != null ) {
                    batch.add(bs);
                }
            }
        }
        LOG.trace(String.format("insert batch statement size=%d", batch.size()));

        try {
            DatastaxIO.getSession().execute(batch);
        } catch ( Exception ex ) {
            Instrumentation.markWriteError();
            LOG.error(String.format("error writing batch of %d metrics", batch.size()), ex );
        }
    }
}
