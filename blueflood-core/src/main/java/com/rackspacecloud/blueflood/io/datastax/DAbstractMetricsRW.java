package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with aspects of reading/writing metrics which are common accross all column families
 * using Datastax driver
 */
public abstract class DAbstractMetricsRW extends AbstractMetricsRW {

    private static final Logger LOG = LoggerFactory.getLogger( DAbstractMetricsRW.class );

    protected final DCounterIO counterIO = new DCounterIO();
    protected final DEnumIO enumIO = new DEnumIO();
    protected final DGagueIO gaugeIO = new DGagueIO();
    protected final DSetIO setIO = new DSetIO();
    protected final DTimerIO timerIO = new DTimerIO();
    protected final LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();
    protected final DBasicNumericIO basicIO = new DBasicNumericIO();
    protected final DSimpleNumberIO simpleIO = new DSimpleNumberIO();

    // a map of RollupType to its IO class that knows
    // how to read/write that particular type of rollup
    protected final Map<RollupType, DAbstractMetricIO> rollupTypeToIO =
            new HashMap<RollupType, DAbstractMetricIO>() {{
                put(RollupType.BF_BASIC, basicIO );
                put(RollupType.COUNTER, counterIO);
                put(RollupType.ENUM, enumIO);
                put(RollupType.GAUGE, gaugeIO);
                put(RollupType.SET, setIO);
                put(RollupType.TIMER, timerIO);
            }};



    /**
     * This method inserts a collection of metrics in the
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
     * objects to the appropriate Cassandra column family
     *
     * @param writeContexts
     * @throws IOException
     */
    @Override
    public void insertRollups(List<SingleRollupWriteContext> writeContexts) throws IOException {

        if (writeContexts.size() == 0) {
            return;
        }

        Map<ResultSetFuture, SingleRollupWriteContext> futureLocatorMap = new HashMap<ResultSetFuture, SingleRollupWriteContext>();
        Timer.Context ctx = Instrumentation.getWriteTimerContext( writeContexts.get( 0 ).getDestinationCF().getName() );
        try {
            for (SingleRollupWriteContext writeContext : writeContexts) {
                Rollup rollup = writeContext.getRollup();
                Locator locator = writeContext.getLocator();
                Granularity granularity = writeContext.getGranularity();
                int ttl = getTtl(locator, rollup.getRollupType(), granularity);

                // lookup the right writer
                RollupType rollupType = writeContext.getRollup().getRollupType();
                DAbstractMetricIO io = rollupTypeToIO.get(rollupType);
                if ( io == null ) {
                    throw new InvalidDataException(
                            String.format("insertRollups(locator=%s, granularity=%s): unsupported rollupType=%s",
                                    locator, granularity, rollupType.name()));
                }

                ResultSetFuture future = io.putAsync(locator, writeContext.getTimestamp(), rollup, writeContext.getGranularity(), ttl);
                futureLocatorMap.put(future, writeContext);
            }

            for (ResultSetFuture future : futureLocatorMap.keySet()) {
                try {
                    future.getUninterruptibly().all();
                } catch (Exception ex) {
                    SingleRollupWriteContext writeContext = futureLocatorMap.get(future);
                    LOG.error(String.format("error writing to locator %s, granularity %s", writeContext.getLocator(), writeContext.getGranularity()), ex);
                }
            }
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
                                            Granularity granularity) throws IOException {
        Map<Locator, MetricData> result = getDatapointsForRange(new ArrayList<Locator>() {{ add(locator); }}, range, granularity);
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
    protected Map<Locator, MetricData> getDatapointsForRange( List<Locator> locators,
                                                              Range range,
                                                              String columnFamily,
                                                              Granularity granularity ) {
        // in this loop, we will fire all the executeAsync() of
        // various select statements, the collect all of the
        // ResultSetFutures
        Map<Locator, List<ResultSetFuture>> locatorToFuturesMap = new HashMap<Locator, List<ResultSetFuture>>();
        Map<Locator, DAbstractMetricIO> locatorIOMap = new HashMap<Locator, DAbstractMetricIO>();

        for (Locator locator : locators) {
            try {


                String type = metadataCache.get( locator, MetricMetadata.TYPE.name().toLowerCase() );
                String rType = metadataCache.get( locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase() );

                DAbstractMetricIO io = getIO( locatorIOMap, locator, type, rType, granularity );

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
                LOG.error(String.format("Error looking up locator %s in cache", locator), ex);
            }
        }
        return resultSetsToMetricData(locatorToFuturesMap, locatorIOMap, granularity);
    }

    /**
     * Given a locator, a data type, a rollup type and granularity, return the correct
     * {@link com.rackspacecloud.blueflood.io.datastax.DAbstractMetricIO} object
     *
     * @param locatorIOMap the Locator -> DabstractMetricIO map to add the new returned object to as a side-effect
     * @param locator
     * @param dataType
     * @param rollupType
     * @param gran
     * @return
     */
    private DAbstractMetricIO getIO( Map<Locator, DAbstractMetricIO> locatorIOMap,
                                     Locator locator,
                                     String dataType,
                                     String rollupType,
                                     Granularity gran ) {

        DAbstractMetricIO io;

        if( rollupType == null && gran == Granularity.FULL ) {

            io = simpleIO;
        }
        else {

            // find out the rollupType for this locator
            RollupType rType = RollupType.fromString( rollupType );

            // get the right PreaggregatedIO class that can process
            // this rollupType
            io = rollupTypeToIO.get(rType);
        }


        if (io == null) {
            throw new IllegalArgumentException(String.format("dont know how to handle locator=%s with rollupType=%s and type=%s", locator, rollupType, dataType));
        }

        // put everything in a map of locator -> io so
        // we can use em up later
        locatorIOMap.put( locator, io );

        return io;
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
                                  String columnFamilyName) throws IOException, CacheException {
        Timer.Context ctx = Instrumentation.getReadTimerContext(columnFamilyName);
        try {
            // read the rollup object from the proper IO class
            DAbstractMetricIO io;

            if( columnFamilyName.equals( CassandraModel.CF_METRICS_FULL_NAME) ) {

                io = simpleIO;
            }
            else
                io = rollupTypeToIO.get(rollupType);

            Table<Locator, Long, Object> locatorTimestampRollup = io.getRollupsForLocator(locator, columnFamilyName, range);

            // transform them to Points
            Points points = new Points();
            for (Table.Cell<Locator, Long, Object> cell : locatorTimestampRollup.cellSet()) {
                points.add( createPoint( cell.getColumnKey(), cell.getValue()));
            }
            return points;
        } finally {
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
     * @param granularity
     * @return
     */
    protected Map<Locator, MetricData> resultSetsToMetricData(Map<Locator, List<ResultSetFuture>> resultSets,
                                                              Map<Locator, DAbstractMetricIO> locatorIO,
                                                              Granularity granularity) {

        // iterate through all ResultSetFuture
        Map<Locator, MetricData> locatorMetricDataMap = new HashMap<Locator, MetricData>();
        for (Map.Entry<Locator, List<ResultSetFuture>> entry : resultSets.entrySet() ) {
            Locator locator = entry.getKey();
            List<ResultSetFuture> futures = entry.getValue();

            try {

                DAbstractMetricIO io = locatorIO.get(locator);


                // get ResultSets to a Table of locator, timestamp, rollup
                Table<Locator, Long, Object> locatorTimestampRollup = io.toLocatorTimestampValue( futures, locator, granularity );

                Map<Long, Object> tsRollupMap = locatorTimestampRollup.row( locator );

                // convert to Points and MetricData
                Points points = convertToPoints( tsRollupMap );

                // get the dataType for this locator
                DataType dataType = getDataType( locator, MetricMetadata.TYPE.name().toLowerCase() );

                RollupType rollupType = getRollupType( tsRollupMap );

                // create MetricData
                MetricData.Type outputType = MetricData.Type.from( rollupType, dataType );
                MetricData metricData = new MetricData( points, getUnitString( locator ), outputType );
                locatorMetricDataMap.put( locator, metricData );

            } catch (CacheException ex) {
                Instrumentation.markReadError();
                LOG.error(String.format("error getting dataType for locator %s, granularity %s",
                        locator, granularity), ex);
            }
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
}
