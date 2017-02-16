package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.annotations.VisibleForTesting;
import com.rackspacecloud.blueflood.cache.LocatorCache;
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

    private DRawIO rawIO = new DRawIO();
    private DSimpleNumberIO simpleIO = new DSimpleNumberIO();
    private final DBasicNumericIO basicIO = new DBasicNumericIO();

    /**
     * Constructor
     * @param isRecordingDelayedMetrics
     */
    @VisibleForTesting
    public DBasicMetricsRW(LocatorIO locatorIO, DelayedLocatorIO delayedLocatorIO,
                           boolean isRecordingDelayedMetrics, Clock clock) {
        super(locatorIO, delayedLocatorIO, isRecordingDelayedMetrics, clock);
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

                Locator locator = metric.getLocator();

                if( !LocatorCache.getInstance().isLocatorCurrentInBatchLayer(locator) ) {

                    LocatorCache.getInstance().setLocatorCurrentInBatchLayer(locator);
                    locatorIO.insertLocator( locator );
                }

                if (isRecordingDelayedMetrics) {
                    insertLocatorIfDelayed(metric);
                }

                futures.put( locator, rawIO.insertAsync( metric ) );

                // this is marking  metrics_strings & metrics_full together.
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
        return simpleIO;
      else
        return basicIO;
    }
}
