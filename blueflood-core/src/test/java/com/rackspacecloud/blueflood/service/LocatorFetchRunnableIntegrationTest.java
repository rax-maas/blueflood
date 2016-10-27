package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.io.astyanax.ABasicMetricsRW;
import com.rackspacecloud.blueflood.io.astyanax.APreaggregatedMetricsRW;
import com.rackspacecloud.blueflood.io.datastax.*;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class LocatorFetchRunnableIntegrationTest extends IntegrationTestBase {

    private static final int MAX_ROLLUP_READ_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS);
    private static final int MAX_ROLLUP_WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_WRITE_THREADS);
    private static int WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.METRICS_BATCH_WRITER_THREADS);
    private static TimeValue timeout = new TimeValue(10, TimeUnit.SECONDS);

    private final ShardStateIO io = IOContainer.fromConfig().getShardStateIO();

    private ScheduleContext ingestionCtx;
    private ShardStateWorker ingestPuller;
    private ShardStateWorker ingestPusher;

    private ScheduleContext rollupCtx;
    private ShardStateWorker rollupPuller;
    private ShardStateWorker rollupPusher;

    private ExecutorService rollupReadExecutor;
    private ThreadPoolExecutor rollupWriteExecutor;
    private ExecutorService enumValidatorExecutor;

    private static final Clock mockClock = mock(Clock.class);


    private final long REF_TIME = 1234000L; //start time of slot 4 for metrics_5m

    private static final long ROLLUP_DELAY_MILLIS = 300000;
    private static final long SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS = 600000;
    private static final long LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS = 500000;

    private static Granularity DELAYED_METRICS_STORAGE_GRANULARITY =
            Granularity.getRollupGranularity(Configuration.getInstance().getStringProperty(CoreConfig.DELAYED_METRICS_STORAGE_GRANULARITY));

    private final List<String> shard1Locators = Arrays.asList(
            "-1.int.perf01.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.131",
            "-100.int.perf01.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.119");
    private final List<String> shard2Locators = Arrays.asList(
            "-1.int.perf01.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.68",
            "-1.int.perf01.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.90",
            "-10.int.perf01.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.121");

    private List<Integer> shards = new ArrayList<Integer>(){{
        add(1);
        add(2);
    }};

    private AbstractMetricsRW basicMetricsRW = new ABasicMetricsRW(true, mockClock);
    private AbstractMetricsRW preAggrMetricsRW = new APreaggregatedMetricsRW(true, mockClock);

    public LocatorFetchRunnableIntegrationTest(AbstractMetricsRW basicMetricsRW, AbstractMetricsRW preAggrMetricsRW) {
        this.basicMetricsRW = basicMetricsRW;
        this.preAggrMetricsRW = preAggrMetricsRW;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getRWs() {

        List<Object[]> rws = new ArrayList<Object[]>();

        AbstractMetricsRW aBasicMetricsRW = new ABasicMetricsRW(true, mockClock);
        AbstractMetricsRW aPreAggrMetricsRW = new APreaggregatedMetricsRW(true, mockClock);

        AbstractMetricsRW dBasicMetricsRW = new DBasicMetricsRW(new DLocatorIO(), new DDelayedLocatorIO(), true, mockClock);
        AbstractMetricsRW dPreAggrMetricsRW = new DPreaggregatedMetricsRW(new DEnumIO(), new DLocatorIO(), new DDelayedLocatorIO(), true, mockClock);

        rws.add(new Object[]{aBasicMetricsRW, aPreAggrMetricsRW});
        rws.add(new Object[]{dBasicMetricsRW, dPreAggrMetricsRW});

        return rws;
    }

    @Before
    @Override
    public void setUp() throws Exception {

        super.setUp();

        final ShardStateIO io = IOContainer.fromConfig().getShardStateIO();

        ingestionCtx = new ScheduleContext(REF_TIME, shards, mockClock);
        rollupCtx = new ScheduleContext(REF_TIME, shards, mockClock);

        // Shard workers for rollup ctx
        rollupPuller = new ShardStatePuller(shards, rollupCtx.getShardStateManager(), this.io);
        rollupPusher = new ShardStatePusher(shards, rollupCtx.getShardStateManager(), this.io);

        // Shard workers for ingest ctx
        ingestPuller = new ShardStatePuller(shards, ingestionCtx.getShardStateManager(), this.io);
        ingestPusher = new ShardStatePusher(shards, ingestionCtx.getShardStateManager(), this.io);

        rollupReadExecutor = new ThreadPoolExecutor(MAX_ROLLUP_READ_THREADS, MAX_ROLLUP_READ_THREADS, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        rollupWriteExecutor = new ThreadPoolExecutor(MAX_ROLLUP_WRITE_THREADS, MAX_ROLLUP_WRITE_THREADS, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        LocatorCache.getInstance().resetCache();

    }

    @Test
    public void testProcessingAllLocatorsForASlot() throws Exception {

        //metrics for slot 4
        final Map<Integer, List<IMetric>> metricsShardMap = generateMetrics(REF_TIME + 1);
        long currentTimeDuringIngest = REF_TIME + 2;

        //inserting metrics corresponding to slot 4 for shards 1 and 2
        ingestMetrics(metricsShardMap, currentTimeDuringIngest);

        rollupPuller.performOperation(); // Shard state is read on rollup host
        rollupCtx.setCurrentTimeMillis(REF_TIME + ROLLUP_DELAY_MILLIS + 10);
        rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS,
                LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        /******************************************************************************************************
         * Rollup of on-time slots                                                                            *
         ******************************************************************************************************/
        when(mockClock.now()).thenReturn(new Instant(REF_TIME + ROLLUP_DELAY_MILLIS));
        List<SlotKey> scheduledSlotKeys = new ArrayList<SlotKey>();
        while (rollupCtx.hasScheduled()) {
            SlotKey slotKey = rollupCtx.getNextScheduled();
            scheduledSlotKeys.add(slotKey);

            LocatorFetchRunnable locatorFetchRunnable = spy(new LocatorFetchRunnable(rollupCtx, slotKey,
                    rollupReadExecutor, rollupWriteExecutor, enumValidatorExecutor));

            RollupExecutionContext rollupExecutionContext = spy(new RollupExecutionContext(Thread.currentThread()));
            RollupBatchWriter rollupBatchWriter = spy(new RollupBatchWriter(rollupWriteExecutor, rollupExecutionContext));
            when(locatorFetchRunnable.createRollupExecutionContext()).thenReturn(rollupExecutionContext);
            when(locatorFetchRunnable.createRollupBatchWriter(any(RollupExecutionContext.class))).thenReturn(rollupBatchWriter);

            locatorFetchRunnable.run();

            //verifying number of locators read and rollups written are same as the number of locators ingested per shard.
            verify(rollupExecutionContext, times(metricsShardMap.get(slotKey.getShard()).size())).incrementReadCounter();
            verify(rollupBatchWriter, times(metricsShardMap.get(slotKey.getShard()).size()))
                    .enqueueRollupForWrite(any(SingleRollupWriteContext.class));

            rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS,
                    LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        }
        assertEquals("Invalid number of slots scheduled", 10, scheduledSlotKeys.size()); //5 for each shard

        for (SlotKey slotKey : scheduledSlotKeys) {
            UpdateStamp updateStamp = rollupCtx.getShardStateManager().getUpdateStamp(slotKey);

            assertEquals("Slot should be in rolled state", UpdateStamp.State.Rolled, updateStamp.getState());
            assertTrue("last rollup time stamp not updated", updateStamp.getLastRollupTimestamp() > 0);
        }

        //ingesting delayed traffic, one delayed metric for shard=1
        //metrics for slot 4
        final int SHARD_1 = 1;
        long currentTimeDuringIngest2 = REF_TIME + ROLLUP_DELAY_MILLIS + 4; //causing delayed metrics
        Map<Integer, List<IMetric>> generatedMetrics = generateMetrics(REF_TIME + 3);
        final IMetric delayedMetric = generatedMetrics.get(SHARD_1).get(0);

        HashMap<Integer, List<IMetric>> delayedMetricsForShard1 = new HashMap<Integer, List<IMetric>>(){{
            put(SHARD_1, new ArrayList<IMetric>(){{ add(delayedMetric); }});
        }};

        //ingesting one delayed metric for shard 1
        ingestMetrics(delayedMetricsForShard1, currentTimeDuringIngest2);

        rollupPuller.performOperation(); // Shard state is read on rollup host
        //last ingest time will be the actual system time as it is the last update time. We adjust it to suit this test
        for (Granularity g: Granularity.rollupGranularities()) {
            UpdateStamp stamp = rollupCtx.getShardStateManager().getSlotStateManager(SHARD_1, g).getSlotStamps().get(g.slot(REF_TIME));
            stamp.setLastIngestTimestamp(currentTimeDuringIngest2);
        }
        rollupCtx.setCurrentTimeMillis(REF_TIME + SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS + 15); //scheduling after short delay
        rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS,
                LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);

        when(mockClock.now()).thenReturn(new Instant(REF_TIME + SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS + 4));

        /******************************************************************************************************
         * Rollup of delayed slots                                                                            *
         ******************************************************************************************************/
        List<SlotKey> scheduledSlotKeys2 = new ArrayList<SlotKey>();
        while (rollupCtx.hasScheduled()) {

            SlotKey slotKey = rollupCtx.getNextScheduled();
            scheduledSlotKeys2.add(slotKey);
            LocatorFetchRunnable locatorFetchRunnable = spy(new LocatorFetchRunnable(rollupCtx, slotKey,
                    rollupReadExecutor, rollupWriteExecutor, enumValidatorExecutor));

            RollupExecutionContext rollupExecutionContext = spy(new RollupExecutionContext(Thread.currentThread()));
            RollupBatchWriter rollupBatchWriter = spy(new RollupBatchWriter(rollupWriteExecutor, rollupExecutionContext));
            when(locatorFetchRunnable.createRollupExecutionContext()).thenReturn(rollupExecutionContext);
            when(locatorFetchRunnable.createRollupBatchWriter(any(RollupExecutionContext.class))).thenReturn(rollupBatchWriter);

            locatorFetchRunnable.run();

            if (slotKey.getGranularity().isCoarser(DELAYED_METRICS_STORAGE_GRANULARITY)) {
                //verifying number of locators read and rollups written are same as the number of delayed locators during re-roll.
                verify(rollupExecutionContext, times(generatedMetrics.get(slotKey.getShard()).size())).incrementReadCounter();
                verify(rollupBatchWriter, times(generatedMetrics.get(slotKey.getShard()).size()))
                        .enqueueRollupForWrite(any(SingleRollupWriteContext.class));
            } else {
                //verifying number of locators read and rollups written are same as the number of delayed locators during re-roll.
                verify(rollupExecutionContext, times(delayedMetricsForShard1.get(slotKey.getShard()).size())).incrementReadCounter();
                verify(rollupBatchWriter, times(delayedMetricsForShard1.get(slotKey.getShard()).size()))
                        .enqueueRollupForWrite(any(SingleRollupWriteContext.class));
            }

            rollupCtx.scheduleEligibleSlots(ROLLUP_DELAY_MILLIS, SHORT_DELAY_METRICS_ROLLUP_DELAY_MILLIS,
                    LONG_DELAY_METRICS_ROLLUP_WAIT_MILLIS);
        }
        assertEquals("Invalid number of slots scheduled", 5, scheduledSlotKeys2.size()); //5 for each shard
    }



    private void ingestMetrics(Map<Integer, List<IMetric>> metricsShardMap, long currentTimeDuringIngest) throws Exception {

        //inserting metrics corresponding to slot 4 for shards 1 and 2
        when(mockClock.now()).thenReturn(new Instant(currentTimeDuringIngest));
        for (int shard: metricsShardMap.keySet()) {

            List<IMetric> batch = metricsShardMap.get(shard);
            basicMetricsRW.insertMetrics(batch);

            for (IMetric metric : batch) {
                ingestionCtx.update(metric.getCollectionTime(), Util.getShard(metric.getLocator().toString()));
            }
        }

        ingestPusher.performOperation(); // Shard state is persisted on ingestion host
    }

    private Map<Integer, List<IMetric>> generateMetrics(final long collectionTime) {
        Map<Integer, List<IMetric>> metricMap = new HashMap<Integer, List<IMetric>>();

        //locator names and tenant id are chosen so that <tenantid>.<locator> belongs to shards 1 and 2.

        //locators corresponding to shard 1
        metricMap.put(1, new ArrayList<IMetric>() {{
            add(generateMetric(shard1Locators.get(0), collectionTime));
            add(generateMetric(shard1Locators.get(1), collectionTime + 1));
        }});

        //locators corresponding to shard 2
        metricMap.put(2, new ArrayList<IMetric>(){{
            add(generateMetric(shard2Locators.get(0), collectionTime + 2));
            add(generateMetric(shard2Locators.get(1), collectionTime + 3));
            add(generateMetric(shard2Locators.get(2), collectionTime + 4));
        }});

        return metricMap;
    }

    private Metric generateMetric(String locator, long collectionTime) {
        return new Metric( Locator.createLocatorFromDbKey(locator),
                System.currentTimeMillis() % 100,
                collectionTime,
                new TimeValue(1, TimeUnit.DAYS),
                "unit" );
    }

}
