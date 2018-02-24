package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with aspects of reading/writing metrics which are common across all column families
 * using Datastax driver
 */
public abstract class DAbstractMetricsRW extends AbstractMetricsRW {

    private static final Logger LOG = LoggerFactory.getLogger(DAbstractMetricsRW.class);

    private static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);

    private static final Granularity DELAYED_METRICS_STORAGE_GRANULARITY =
            Granularity.getRollupGranularity(Configuration.getInstance().getStringProperty(CoreConfig.DELAYED_METRICS_STORAGE_GRANULARITY));

    protected final DLocatorIO locatorIO;
    protected final DDelayedLocatorIO delayedLocatorIO;
    protected final boolean isBatchIngestEnabled;

    /**
     * Constructor
     * @param locatorIO
     * @param isRecordingDelayedMetrics
     */
    protected DAbstractMetricsRW(DLocatorIO locatorIO, DDelayedLocatorIO delayedLocatorIO,
                                 boolean isRecordingDelayedMetrics,
                                 Clock clock) {
        this.isRecordingDelayedMetrics = isRecordingDelayedMetrics;
        this.isBatchIngestEnabled = false;
        this.locatorIO = locatorIO;
        this.delayedLocatorIO = delayedLocatorIO;
        this.clock = clock;
    }

    /**
     * Constructor
     * @param locatorIO
     * @param isRecordingDelayedMetrics
     */
    @VisibleForTesting
    protected DAbstractMetricsRW(DLocatorIO locatorIO, DDelayedLocatorIO delayedLocatorIO,
                                 boolean isRecordingDelayedMetrics,
                                 boolean isBatchIngestEnabled, Clock clock) {
        this.isRecordingDelayedMetrics = isRecordingDelayedMetrics;
        this.isBatchIngestEnabled = isBatchIngestEnabled;
        this.locatorIO = locatorIO;
        this.delayedLocatorIO = delayedLocatorIO;
        this.clock = clock;
    }

    /**
     * Return the appropriate IO object which interacts with the Cassandra database.
     *
     * For Preaggregated Metrics, only rollup type is required.
     * For Basic Numeric Metrics, the granularity is required as metrics_full is handled differently
     * than the other granularities.
     *
     * @param rollupType
     * @param gran
     * @return
     */
    protected abstract DAbstractMetricIO getIO( String rollupType, Granularity gran );

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext} objects
     * to the appropriate Cassandra column family.
     *
     * It performs the inserts by executing an UNLOGGED BATCH statement.
     *
     * @param writeContexts
     *
     * @throws IOException
     */
    @Override
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) {

        if (writeContexts.size() == 0) {
            return;
        }

        Timer.Context ctx = Instrumentation.getWriteTimerContext( writeContexts.get( 0 ).getDestinationCF().getName() );
        try {

            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);

            for (SingleRollupWriteContext writeContext : writeContexts) {
                Rollup rollup = writeContext.getRollup();
                Locator locator = writeContext.getLocator();
                Granularity granularity = writeContext.getGranularity();
                int ttl = getTtl(locator, rollup.getRollupType(), granularity);

                // lookup the right writer
                RollupType rollupType = writeContext.getRollup().getRollupType();
                DAbstractMetricIO io = getIO(rollupType.name().toLowerCase(), granularity);

                Statement statement = io.createStatement(locator, writeContext.getTimestamp(), rollup, writeContext.getGranularity(), ttl);
                batch.add(statement);
            }

            Session session = DatastaxIO.getSession();
            session.execute(batch);

        } catch (Exception ex) {
            Instrumentation.markWriteError();
            LOG.error(String.format("error writing locator batch of size %s, granularity %s", writeContexts.size(), writeContexts.get(0).getGranularity()), ex);
        } finally {
            ctx.stop();
        }
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locator
     * @param range
     * @param granularity
     * @return
     */
    @Override
    public MetricData getDatapointsForRange(final Locator locator,
                                            Range range,
                                            Granularity granularity) {
        Map<Locator, MetricData> result = getDatapointsForRange(new ArrayList<Locator>() {{
            add(locator);
        }}, range, granularity);
        return result.get(locator);
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * This is a helper method used to get basic metrics as well as preaggregated metrics.  Hence, both granularity
     * and columnFamily are required.
     *
     * @param locators
     * @param range
     * @param columnFamily
     * @param granularity
     * @return
     */
    public Map<Locator, MetricData> getDatapointsForRange( List<Locator> locators,
                                                              Range range,
                                                              String columnFamily,
                                                              Granularity granularity ) {

        Timer.Context ctx = Instrumentation.getReadTimerContext(columnFamily);

        try {

            MetadataCache metadataCache = MetadataCache.getInstance();

            // in this loop, we will fire all the executeAsync() of
            // various select statements, the collect all of the
            // ResultSetFutures
            Map<Locator, List<ResultSetFuture>> locatorToFuturesMap = new HashMap<Locator, List<ResultSetFuture>>();
            Map<Locator, DAbstractMetricIO> locatorIOMap = new HashMap<Locator, DAbstractMetricIO>();

            for (Locator locator : locators) {
                try {

                    String rType = metadataCache.get(locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase());

                    DAbstractMetricIO io = getIO( rType, granularity );

                    // put everything in a map of locator -> io so
                    // we can use em up later
                    locatorIOMap.put( locator, io );

                    // do the query
                    List<ResultSetFuture> selectFutures = io.selectForLocatorAndRange( columnFamily, locator, range );

                    // add all ResultSetFutures for a particular locator together
                    List<ResultSetFuture> existing = locatorToFuturesMap.get(locator);
                    if (existing == null) {
                        existing = new ArrayList<ResultSetFuture>();
                        locatorToFuturesMap.put(locator, existing);
                    }
                    existing.addAll(selectFutures);

                } catch (CacheException ex) {
                    Instrumentation.markReadError();
                    LOG.error(String.format("Error looking up locator %s in cache", locator), ex);
                }
            }
            return resultSetsToMetricData(locatorToFuturesMap, locatorIOMap, columnFamily, range);
        }
        finally {

            ctx.stop();
        }
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
    public Points getDataToRollup(final Locator locator,
                                  RollupType rollupType,
                                  Range range,
                                  String columnFamilyName) throws IOException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(columnFamilyName);
        try {
            // read the rollup object from the proper IO class
            DAbstractMetricIO io = getIO( rollupType.name().toLowerCase(), CassandraModel.getGranularity( columnFamilyName ) );

            Table<Locator, Long, Object> locatorTimestampRollup = io.getRollupsForLocator( locator, columnFamilyName, range );

            Points points = new Points();
            for (Table.Cell<Locator, Long, Object> cell : locatorTimestampRollup.cellSet()) {
                points.add( createPoint( cell.getColumnKey(), cell.getValue()));
            }
            return points;
        } catch( Exception e ) {

            Instrumentation.markReadError();
            LOG.error( String.format( "Unable to read locator=%s rolluptype=%s columnFamilyName=%s for rollup",
                    locator, rollupType.name(), columnFamilyName ), e );

            throw new IOException( e );
        }
        finally {
            ctx.stop();
        }
    }

    /**
     * Converts a list of {@link com.datastax.driver.core.ResultSetFuture} for each
     * {@link com.rackspacecloud.blueflood.types.Locator} to
     * {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} object.
     *
     * @param resultSets
     * @param locatorIO
     * @param columnFamily
     * @return
     */
    protected Map<Locator, MetricData> resultSetsToMetricData(Map<Locator, List<ResultSetFuture>> resultSets,
                                                              Map<Locator, DAbstractMetricIO> locatorIO,
                                                              String columnFamily,
                                                              Range range) {

        MetadataCache metadataCache = MetadataCache.getInstance();

        // iterate through all ResultSetFuture
        Map<Locator, MetricData> locatorMetricDataMap = new HashMap<Locator, MetricData>();
        for (Map.Entry<Locator, List<ResultSetFuture>> entry : resultSets.entrySet() ) {
            Locator locator = entry.getKey();
            List<ResultSetFuture> futures = entry.getValue();

            DAbstractMetricIO io = locatorIO.get(locator);

            // get ResultSets to a Table of locator, timestamp, rollup
            Table<Locator, Long, Object> locatorTimestampRollup = io.toLocatorTimestampValue(futures, locator, columnFamily, range);

            Map<Long, Object> tsRollupMap = locatorTimestampRollup.row( locator );

            // convert to Points and MetricData
            Points points = convertToPoints( tsRollupMap );

            // create MetricData
            MetricData metricData = new MetricData( points, metadataCache.getUnitString( locator ) );
            locatorMetricDataMap.put( locator, metricData );
        }
        return locatorMetricDataMap;
    }

    /**
     * Given a map of timestamps -> Rollups/SimpleNumbers, return RollupType, or null, if not a Rollup.
     *
     * @param tsRollupMap
     * @return
     */
    private RollupType getRollupType( Map<Long, Object> tsRollupMap ) {

        if( tsRollupMap.isEmpty() )
            return null;
        else {

            Object value = tsRollupMap.values().iterator().next();
            return value instanceof Rollup ? ( (Rollup) value ).getRollupType() : null;
        }
    }

    /**
     * Determines if the metric is considered delayed or not
     *
     * @param metric
     * @return
     */
    protected boolean isDelayed(IMetric metric) {
        long delay = clock.now().getMillis() - metric.getCollectionTime();
        return delay > MAX_AGE_ALLOWED;
    }

    /**
     * Returns the delayed slot for a metric
     *
     * @param metric
     * @return
     */
    protected int getDelayedSlot(IMetric metric) {
        //track locator for configured granularity level. to re-roll only the delayed locator's for that slot
        return DELAYED_METRICS_STORAGE_GRANULARITY.slot(metric.getCollectionTime());
    }

    /**
     * This method inserts the locator into the metric_delayed_locator column family, if the metric is delayed.
     *
     * @param metric
     * @throws IOException
     */
    protected void insertLocatorIfDelayed(IMetric metric) throws IOException {
        Locator locator = metric.getLocator();
        if ( isDelayed(metric) ) {
            int slot = getDelayedSlot(metric);
            if (!LocatorCache.getInstance().isDelayedLocatorForASlotCurrent(slot, locator)) {
                delayedLocatorIO.insertLocator(DELAYED_METRICS_STORAGE_GRANULARITY, slot, locator);
                LocatorCache.getInstance().setDelayedLocatorForASlotCurrent(slot, locator);
            }
        }
    }

    /**
     * Returns a BoundStatement if a metric needs to be inserted to the metrics_delayed_locator
     * Column Family. Returns null otherwise.
     *
     * @param metric
     * @return
     */
    protected BoundStatement getBoundStatementForMetricIfDelayed(IMetric metric) {
        Locator locator = metric.getLocator();
        if ( isDelayed(metric) ) {
            int slot = getDelayedSlot(metric);
            if (!LocatorCache.getInstance().isDelayedLocatorForASlotCurrent(slot, locator)) {
                LocatorCache.getInstance().setDelayedLocatorForASlotCurrent(slot, locator);
                return delayedLocatorIO.getBoundStatementForLocator(DELAYED_METRICS_STORAGE_GRANULARITY, slot, locator);
            }
        }
        return null;
    }
}
