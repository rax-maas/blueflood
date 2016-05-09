package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Result;
import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the basic metrics_* and metrics_string column families
 * using Datastax driver
 */
public class DBasicMetricsRW extends DAbstractMetricsRW {

    public static final String COLUMN1 = "column1";
    public static final String VALUE = "value";

    private static final Logger LOG = LoggerFactory.getLogger( DBasicMetricsRW.class );

    private boolean areStringMetricsDropped = Configuration.getInstance().getBooleanProperty( CoreConfig.STRING_METRICS_DROPPED);
    private List<String> tenantIdsKept = Configuration.getInstance().getListProperty(CoreConfig.TENANTIDS_TO_KEEP);
    private Set<String> keptTenantIdsSet = new HashSet<String>(tenantIdsKept);

    private DRawIO rawIO = new DRawIO();
    private LocatorIO locatorIO = IOContainer.fromConfig().getLocatorIO();

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
                    LOG.trace("Metric shouldn't be persisted, skipping insert", metric.getLocator().toString());
                    continue;
                }

                Locator locator = metric.getLocator();

                if( !isLocatorCurrent( locator ) ) {

                    setLocatorCurrent( locator );

                    if( !DataType.isStringOrBoolean( metric.getMetricValue() ) )
                        locatorIO.insertLocator( locator );
                }


                futures.put( locator, rawIO.insertAsync( metric ) );
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
        finally {

            ctx.close();
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
    public MetricData getDatapointsForRange( final Locator locator, Range range, Granularity gran ) throws IOException {

        Map<Locator, MetricData> result = getDatapointsForRange( new ArrayList<Locator>() {{ add( locator ); }}, range, gran );
        return result.get( locator );
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
    public Map<Locator, MetricData> getDatapointsForRange( List<Locator> locators, Range range, Granularity gran ) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext( "DBasicMetricsRW.getDatapointsForRange" );
        try {

            List<Locator> unknowns = new ArrayList<Locator>();
            List<Locator> strings = new ArrayList<Locator>();
            List<Locator> booleans = new ArrayList<Locator>();
            List<Locator> numerics = new ArrayList<Locator>();

            for ( Locator locator : locators ) {

                Object type;
                try {

                    type = metadataCache.get( locator, DATA_TYPE_CACHE_KEY );

                } catch ( CacheException e ) {

                    LOG.error( "Unable to read cache", e );

                    unknowns.add( locator );
                    continue;
                }


                DataType metricType = new DataType( (String) type );

                if ( type == null || !DataType.isKnownMetricType( metricType ) ) {

                    unknowns.add( locator );
                    continue;
                } else if ( metricType.equals( DataType.STRING ) ) {

                    strings.add( locator );
                    continue;
                } else if ( metricType.equals( DataType.BOOLEAN ) ) {

                    booleans.add( locator );
                    continue;
                } else {

                    numerics.add( locator );
                }
            }

            Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();

            String columnFamily = CassandraModel.getBasicColumnFamilyName( gran );

            metrics.putAll( super.getDatapointsForRange( numerics, range, columnFamily, gran ) );
            metrics.putAll( getBooleanDataForRange( booleans, range ) );
            metrics.putAll( getStringDataForRange( strings, range ) );
            metrics.putAll( getNumericOrStringDataForRange( unknowns, range, columnFamily, gran ) );

            return metrics;
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
                                  String columnFamilyName) throws IOException, CacheException {

        if( columnFamilyName.equals ( CassandraModel.CF_METRICS_STRING_NAME ) ) {

            String msg = String.format( String.format( "DBasicMetricsRW.getDataToRollup: %s Attempting to write String/Boolean metric for a method which should not get them.",
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

        Map<Locator, MetricData> metrics = new HashMap<Locator, MetricData>();
        Map<Locator, ResultSetFuture> futures = new HashMap<Locator, ResultSetFuture>();

        for( Locator locator : locators ) {
            futures.put( locator, rawIO.getStringAsync( locator, range ) );
        }

        for( Map.Entry<Locator, ResultSetFuture> future : futures.entrySet() ) {

            Points points = new Points<String>();

            try {
                for ( Row row : future.getValue().getUninterruptibly().all() ) {

                    Object value = isBoolean ? Boolean.valueOf( row.getString( VALUE ) ) : row.getString( VALUE );
                    points.add( new Points.Point( row.getLong( COLUMN1 ), value ) );
                }

                MetricData.Type type = isBoolean ? MetricData.Type.BOOLEAN : MetricData.Type.BOOLEAN;

                metrics.put( future.getKey(), new MetricData( points, getUnitString( future.getKey() ), type ) );
            }
            catch (Exception e ) {

                LOG.error( String.format( "error writing metric %s, metrics_string", future.getKey() ), e );
            }
        }

        return metrics;
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
                                                                     Granularity gran ) throws IOException {

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
}
