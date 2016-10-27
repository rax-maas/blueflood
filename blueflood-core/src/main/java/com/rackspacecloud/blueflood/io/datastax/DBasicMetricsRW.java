package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the basic metrics_* and metrics_string column families
 * using Datastax driver
 */
public class DBasicMetricsRW extends DAbstractMetricsRW {

    private static final Logger LOG = LoggerFactory.getLogger(DBasicMetricsRW.class);
    private static final Timer waitResultsTimer = Metrics.timer(DBasicMetricsRW.class, "Basic Metrics Wait Write Results");

    private boolean areStringMetricsDropped;
    private Set<String> keptTenantIdsSet;

    private DRawIO rawIO = new DRawIO();
    private DSimpleNumberIO simpleIO = new DSimpleNumberIO();
    private final DBasicNumericIO basicIO = new DBasicNumericIO();

    /**
     * Constructor
     * By default constructing DBasicMetricsRW this way will allow
     * ingestion of String metrics for all tenants.
     * See #DBasicMetricsRW(boolean, List) to change the behavior.
     */
    @VisibleForTesting
    public DBasicMetricsRW(LocatorIO locatorIO, DelayedLocatorIO delayedLocatorIO, boolean isRecordingDelayedMetrics, Clock clock) {
        this(locatorIO, delayedLocatorIO, false, new ArrayList<String>(), isRecordingDelayedMetrics, clock);
    }

    /**
     * Constructor
     *  @param ignoreStringMetrics
     * @param tenantIdsKept
     * @param isRecordingDelayedMetrics
     */
    public DBasicMetricsRW(LocatorIO locatorIO, DelayedLocatorIO delayedLocatorIO, boolean ignoreStringMetrics,
                           List<String> tenantIdsKept, boolean isRecordingDelayedMetrics, Clock clock) {
        super(locatorIO, delayedLocatorIO, isRecordingDelayedMetrics, clock);
        this.areStringMetricsDropped = ignoreStringMetrics;
        this.keptTenantIdsSet  = new HashSet<String>(tenantIdsKept);
    }

    /**
     * This method inserts a collection of {@link com.rackspacecloud.blueflood.types.IMetric} objects
     * to the appropriate Cassandra column family.
     *
     * @param metrics
     *
     * @throws IOException
     */
    @Override
    public void insertMetrics( Collection<IMetric> metrics ) throws IOException {
        // if there are strings & booleans in this request, they will be tracked as numeric metrics.
        // not sure how we want to get around that.
        Timer.Context ctx = Instrumentation.getWriteTimerContext( CassandraModel.CF_METRICS_FULL_NAME );

        Map<Locator, ResultSetFuture> futures = new HashMap<Locator, ResultSetFuture>();

        try {

            for( IMetric metric : metrics ) {

                if (!shouldPersist(metric)) {
                    LOG.trace(String.format("Metric %s shouldn't be persisted, skipping insert", metric.getLocator().toString()));
                    continue;
                }

                Locator locator = metric.getLocator();

                if( !LocatorCache.getInstance().isLocatorCurrent(locator) ) {

                    LocatorCache.getInstance().setLocatorCurrent(locator);

                    if( !DataType.isStringOrBoolean( metric.getMetricValue() ) )
                        locatorIO.insertLocator( locator );
                }

                if (isRecordingDelayedMetrics) {
                    //retaining the same conditional logic that was used to insertLocator(locator) above.
                    if (!DataType.isStringOrBoolean(metric.getMetricValue())) {
                        insertLocatorIfDelayed(metric);
                    }
                }

                futures.put( locator, rawIO.insertAsync( metric ) );
            }

            for( Map.Entry<Locator, ResultSetFuture> f : futures.entrySet() ) {

                try {
                    ResultSet result = f.getValue().getUninterruptibly();

                    LOG.trace( "result.size=" + result.all().size() );
                    // this is marking  metrics_strings & metrics_full together.
                    Instrumentation.markFullResMetricWritten();
                }
                catch ( Exception e ) {
                    Instrumentation.markWriteError();
                    LOG.error(String.format("error writing metric for locator %s",
                            f.getKey()), e );
                }
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

        List<Locator> unknowns = new ArrayList<Locator>();
        List<Locator> strings = new ArrayList<Locator>();
        List<Locator> booleans = new ArrayList<Locator>();
        List<Locator> numerics = new ArrayList<Locator>();

        for ( Locator locator : locators ) {

            DataType metricType;
            try {
                metricType = getDataType(locator);
            } catch ( CacheException e ) {
                LOG.error(String.format("Error looking up locator %s in cache", locator), e);
                unknowns.add( locator );
                continue;
            }

            if ( !DataType.isKnownMetricType( metricType ) ) {
                unknowns.add( locator );
                continue;
            } else if ( metricType.equals( DataType.STRING ) ) {
                strings.add( locator );
                continue;
            } else if ( metricType.equals( DataType.BOOLEAN ) ) {
                booleans.add( locator );
                continue;
            } else {
                // numeric goes here
                numerics.add( locator );
            }
        }

        Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();

        String columnFamily = CassandraModel.getBasicColumnFamilyName( gran );

        metrics.putAll( super.getDatapointsForRange( numerics, range, columnFamily, gran));
            metrics.putAll(getBooleanDataForRange(booleans, range));
        metrics.putAll( getStringDataForRange( strings, range ) );
        metrics.putAll( getNumericOrStringDataForRange( unknowns, range, columnFamily, gran ) );

        return metrics;
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

        if( columnFamilyName.equals ( CassandraModel.CF_METRICS_STRING_NAME ) ) {

            String msg = String.format( String.format( "DBasicMetricsRW.getDataToRollup: %s Attempting to read String/Boolean metric for a method which should not get them.",
                    columnFamilyName ) );

            LOG.error( msg );

            throw new IOException( msg );
        }

        return super.getDataToRollup( locator, rollupType, range, columnFamilyName );
    }

    /**
     * Returns true if the metric should be persisted.
     *
     * Blueflood can be configured not to save strings, and to only keep strings for specific tenants.  Fun!
     *
     * @param metric
     * @return
     */
    private boolean shouldPersist( IMetric metric ) {

        String tenantId = metric.getLocator().getTenantId();
        boolean stringOrBool = DataType.isStringOrBoolean( metric.getMetricValue() );

        if( stringOrBool && areStringMetricsDropped && !keptTenantIdsSet.contains( tenantId ) ) {
            return false;
        }
        else if( stringOrBool ) {

            String currentValue = String.valueOf(metric.getMetricValue());
            final String lastValue = rawIO.getLastStringValue( metric.getLocator() );

            return lastValue == null || !currentValue.equals(lastValue);
        }

        return true;
    }

    /**
     *  Return map of MetricDatas for string values in the given range.
     *
     * @param locators
     * @param range
     * @return
     */
    private Map<Locator, MetricData> getStringDataForRange( List<Locator> locators, Range range ) {

        return getStringBooleanDataForRange( locators, range, false );
    }

    /**
     * Return map of MetricDatas for strings or booleans in the given range, depending on the isBoolean param.
     *
     * @param locators
     * @param range
     * @param isBoolean if true, return MetricDatas as type Boolean
     * @return
     */
    private Map<Locator, MetricData> getStringBooleanDataForRange( List<Locator> locators, Range range, boolean isBoolean ) {


        Timer.Context ctx = Instrumentation.getReadTimerContext( CassandraModel.CF_METRICS_STRING_NAME );

        MetadataCache metadataCache = MetadataCache.getInstance();
        try {

            Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();
            Map<Locator, ResultSetFuture> futures = new HashMap<Locator, ResultSetFuture>();

            for( Locator locator : locators ) {
                futures.put( locator, rawIO.getStringAsync( locator, range ) );
            }

            for( Map.Entry<Locator, ResultSetFuture> future : futures.entrySet() ) {

                try {

                    metrics.put( future.getKey(), rawIO.createMetricDataStringBoolean( future.getValue(),
                            isBoolean, metadataCache.getUnitString(future.getKey()) ) );
                }
                catch (Exception e ) {

                    Instrumentation.markReadError();

                    LOG.error( String.format( "error reading metric %s, metrics_string", future.getKey() ), e );
                }
            }

            return metrics;

        }
        finally {

            ctx.stop();
        }
    }

    /**
     * Return map of MetricDatas for boolean values in the given range.
     *
     *
     * @param locators
     * @param range
     * @return
     */
    private Map<Locator, MetricData> getBooleanDataForRange( List<Locator> locators, Range range ) {

        return getStringBooleanDataForRange( locators, range, true );
    }

    /**
     * When the type is not known for a list of locators, do the following:
     * 1) first try to get them as numerics.
     * 2) If that call is empty, see if they are in metrics_string (ignore granularity & columnFamily)
     *
     * @param locators
     * @param range
     * @param columnFamily
     * @param gran
     * @return
     * @throws IOException
     */
    private Map<Locator, MetricData> getNumericOrStringDataForRange( List<Locator> locators,
                                                                     Range range,
                                                                     String columnFamily,
                                                                     Granularity gran ) {

        Instrumentation.markScanAllColumnFamilies();

        Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();

        for ( final Locator locator : locators ) {

            // I'm not calling getDatapointsForRange( Locator, ... ) as it calls getDatapointsForRange( List<Locator> ...)
            // which is overridden by this class
            MetricData data = super.getDatapointsForRange( new ArrayList<Locator>() {{ add( locator ); }}, range, columnFamily, gran ).get( locator );

            if ( !data.getData().getPoints().isEmpty() ) {

                metrics.put( locator, data );
            } else {
                metrics.putAll( getStringDataForRange( new ArrayList<Locator>() {{ add( locator ); }}, range ) );
            }
        }

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
        return simpleIO;
      else
        return basicIO;
    }
}
