package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.io.astyanax.ABasicMetricsRW;
import com.rackspacecloud.blueflood.io.datastax.DBasicMetricsRW;
import com.rackspacecloud.blueflood.io.datastax.DDelayedLocatorIO;
import com.rackspacecloud.blueflood.io.datastax.DLocatorIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SingleRollupWriteContext;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import org.junit.Before;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Base class for gesting BasicMetricsRW implementations for writing/reading basic metrics (SimpleNumber, String,
 * Booleans).  This class mostly creates:
 * <ul>
 *  <li>the sample data which is read and written using the MetricsRW implementations
 *  <li>helper methods for creating {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
 * </ul>
 */
public class BasicMetricsRWIntegrationTest extends IntegrationTestBase {

    protected static final double EPSILON = .5;

    private static final String TENANT1 = "123456";
    private static final String TENANT2 = "987654";
    private static final String TENANT3 = "123789";
    private static final TimeValue TTL = new TimeValue(24, TimeUnit.HOURS);

    protected LocatorIO locatorIO = new DLocatorIO();
    protected DelayedLocatorIO delayedLocatorIO = new DDelayedLocatorIO();

    protected MetricsRW datastaxMetricsRW = new DBasicMetricsRW(locatorIO, delayedLocatorIO, false, new DefaultClockImpl());
    protected MetricsRW astyanaxMetricsRW = new ABasicMetricsRW(false, new DefaultClockImpl());

    protected Map<Locator, IMetric> numericMap = new HashMap<Locator, IMetric>();
    protected Map<Locator, IMetric> stringMap = new HashMap<Locator, IMetric>();
    protected Map<Locator, IMetric> boolMap = new HashMap<Locator, IMetric>();

    protected static final long MAX_AGE_ALLOWED = Configuration.getInstance().getLongProperty(CoreConfig.ROLLUP_DELAY_MILLIS);

    protected static Granularity DELAYED_METRICS_STORAGE_GRANULARITY =
            Granularity.getRollupGranularity(Configuration.getInstance().getStringProperty(CoreConfig.DELAYED_METRICS_STORAGE_GRANULARITY));

    /**
     * Generate numeric, string and boolean metrics to be used by the tests.
     *
     * @throws CacheException
     */
    @Before
    public void generateMetrics() throws CacheException {

        String className = getClass().getSimpleName();

        for( String tid : Arrays.asList( TENANT1, TENANT2, TENANT3 ) ) {

            // Numeric
            Locator locator = Locator.createLocatorFromPathComponents( tid, className + ".numeric.metric." + System.currentTimeMillis() );
            Metric metric = new Metric( locator,
                    new Long( System.currentTimeMillis() % 100  ),
                    System.currentTimeMillis(),
                    new TimeValue(1, TimeUnit.DAYS),
                    "unit" );
            numericMap.put( locator, metric );
            MetadataCache.getInstance().put( locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.NUMERIC.toString() );


            // String
            locator = Locator.createLocatorFromPathComponents( tid, className + ".string.metric." + System.currentTimeMillis() );
            metric = new Metric( locator,
                    "String_value." + (System.currentTimeMillis() % 100),
                    System.currentTimeMillis(),
                    new TimeValue(1, TimeUnit.DAYS), "unit" );
            stringMap.put( locator, metric );
            MetadataCache.getInstance().put( locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.STRING.toString() );

            // Boolean
            locator = Locator.createLocatorFromPathComponents( tid, className + ".boolean.metric." + System.currentTimeMillis() );
            metric = new Metric( locator,
                    System.currentTimeMillis() % 2 == 0 ?  true : false,
                    System.currentTimeMillis(),
                    new TimeValue(1, TimeUnit.DAYS), "unit" );
            boolMap.put( locator, metric );
            MetadataCache.getInstance().put( locator, MetricMetadata.TYPE.name().toLowerCase(), DataType.BOOLEAN.toString() );

        }
    }

    /**
     * This method is to supply the granularity parameter to some test methods below
     *
     * @return
     */
    protected Object getGranularitiesToTest() {
        return Arrays.copyOfRange( Granularity.granularities(), 1, Granularity.granularities().length - 1);
    }

    /**
     * Converts the input metrics from a map of locator -> IMetric to a list of
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext}
     * objects
     *
     * @param inputMetrics
     * @return
     */
    protected List<SingleRollupWriteContext> toWriteContext( Collection<IMetric> inputMetrics, Granularity destGran) throws IOException {

        List<SingleRollupWriteContext> resultList = new ArrayList<SingleRollupWriteContext>();
        for ( IMetric metric : inputMetrics ) {

            SingleRollupWriteContext writeContext = createSingleRollupWriteContext( destGran, metric );
            resultList.add(writeContext);
        }
        return resultList;

    }

    /**
     * Convert a list of {@link com.rackspacecloud.blueflood.types.Points} into a list of
     * {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext} for the given
     * Granularity and Locator.
     *
     * @param locator
     * @param points
     * @param gran
     * @return
     */
    protected List<SingleRollupWriteContext> toWriteContext( Locator locator, Points<Rollup> points, Granularity gran ) {

        List<SingleRollupWriteContext> resultList = new ArrayList<SingleRollupWriteContext>();

        for( Map.Entry<Long, Points.Point<Rollup>> entry : points.getPoints().entrySet() ) {

            resultList.add( new SingleRollupWriteContext(
                    entry.getValue().getData(),
                    locator,
                    gran,
                    CassandraModel.getBasicColumnFamily( gran ),
                    entry.getKey() ) );
        }

        return resultList;
    }

    /**
     * Create a single {@link com.rackspacecloud.blueflood.service.SingleRollupWriteContext} from the given
     * {@link com.rackspacecloud.blueflood.types.IMetric} and Granularity.
     *
     * @param destGran
     * @param metric
     * @return
     * @throws IOException
     */
    protected SingleRollupWriteContext createSingleRollupWriteContext( Granularity destGran, IMetric metric ) throws IOException {

        Locator locator = metric.getLocator();

        Points<SimpleNumber> points = new Points<SimpleNumber>();
        points.add( new Points.Point<SimpleNumber>( metric.getCollectionTime(), new SimpleNumber( metric.getMetricValue() ) ) );

        BasicRollup rollup = BasicRollup.buildRollupFromRawSamples( points );

        return new SingleRollupWriteContext(
                rollup,
                locator,
                destGran,
                CassandraModel.getBasicColumnFamily( destGran ),
                metric.getCollectionTime());
    }


    /**
     * For a given list of locators, figure the shard they belong to and for all those shards
     * get all the locators in metric_locator column family
     *
     * @param ingestedLocators
     * @return
     * @throws IOException
     */
    protected Set<Locator> retrieveLocators(Set<Locator> ingestedLocators) throws IOException {

        Set<Long> shards = new HashSet<Long>();
        for (Locator locator: ingestedLocators) {
            long shard = (long) Util.getShard(locator.toString());
            shards.add(shard);
        }

        Set<Locator> locatorsFromDB = new HashSet<Locator>();
        for (Long shard: shards) {
            locatorsFromDB.addAll(locatorIO.getLocators(shard));
        }

        return locatorsFromDB;
    }

    /**
     * For a given list of metrics, figure out the shard and slot they belong to and for those
     * shard and slot combinations, get all the locators from metrics_delayed_locator column family.
     *
     * @param metrics
     * @return
     * @throws IOException
     */
    protected Set<Locator> retrieveLocatorsByShardAndSlot(List<IMetric> metrics) throws IOException {
        Set<String> slotKeys = new HashSet<String>();

        for (IMetric metric: metrics) {
            int shard = Util.getShard(metric.getLocator().toString());
            int slot = DELAYED_METRICS_STORAGE_GRANULARITY.slot(metric.getCollectionTime());
            SlotKey slotKey = SlotKey.of(DELAYED_METRICS_STORAGE_GRANULARITY, slot, shard);

            slotKeys.add(slotKey.toString());
        }

        Set<Locator> locatorsFromDB = new HashSet<Locator>();
        for (String slotKeyStr:  slotKeys) {
            locatorsFromDB.addAll(delayedLocatorIO.getLocators(SlotKey.parse(slotKeyStr)));
        }

        return locatorsFromDB;
    }

}
