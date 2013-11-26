/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BatchMetricsQuery;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchMetricsQueryHandler {
    private static final Logger log = LoggerFactory.getLogger(BatchMetricsQueryHandler.class);

    private final AstyanaxReader reader;
    private final ThreadPoolExecutor executor;
    private final Map<Locator, MetricData> queryResults;
    private AtomicInteger failedReads;
    private static final Timer queryTimer = Metrics.newTimer(Instrumentation.class,
            "Batched Metrics Query Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Meter exceededQueryTimeout = Metrics.newMeter(Instrumentation.class,
            "Batched Metrics Query Duration Exceeded Timeout", "Rollups", TimeUnit.SECONDS);
    private static final Timer singleFetchTimer = Metrics.newTimer(Instrumentation.class,
            "Single Metric Fetch Duration", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private static final Histogram failedQueriesHist = Metrics.newHistogram(Instrumentation.class,
            "Failed queries", true);
    private static final Histogram queriesSizeHist = Metrics.newHistogram(Instrumentation.class,
            "Total queries", true);

    public BatchMetricsQueryHandler(ThreadPoolExecutor executor, AstyanaxReader reader) {
        this.executor = executor;
        this.reader = reader;
        this.queryResults = new ConcurrentHashMap<Locator, MetricData>();
        this.failedReads = new AtomicInteger(0);
    }

    public Map<Locator, MetricData> execute(final BatchMetricsQuery query, TimeValue queryTimeout)
            throws Exception {
        final CountDownLatch shortLatch = new CountDownLatch(query.getLocators().size());
        final TimerContext queryTimerCtx = queryTimer.time();

        final List<Future<Boolean>> resultFutures = new ArrayList<Future<Boolean>>();
        for (final Locator locator : query.getLocators()) {
            Future<Boolean> result = executor.submit(
                    new MetricFetchCallable(locator,
                                            query.getRange(),
                                            query.getGranularity(),
                                            shortLatch)
            );
            resultFutures.add(result);
        }

        // Wait until timeout happens or you got all the results
        shortLatch.await(queryTimeout.getValue(), queryTimeout.getUnit());
        queryTimerCtx.stop();

        long inProgress = query.getLocators().size() - queryResults.size();

        if (failedReads.get() > 0) {
            log.error("Failed reading data for " + failedReads + " out of " + query.getLocators().size() + " metrics");
        }

        if (inProgress > 0) {
            for (Future<Boolean> future : resultFutures) {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }
            log.warn("Interrupted metric fetch for " + inProgress + " out of " + query.getLocators().size() + " metrics");
            exceededQueryTimeout.mark();
            executor.purge();
        }

        failedQueriesHist.update(failedReads.get());
        queriesSizeHist.update(query.getLocators().size());

        return queryResults;
    }


    public class MetricFetchCallable implements Callable<Boolean> {
        private final Locator locator;
        private final Range range;
        private final Granularity gran;
        private final CountDownLatch latch;

        public MetricFetchCallable(Locator locator, Range range, Granularity gran, CountDownLatch latch) {
            this.locator = locator;
            this.range = range;
            this.gran = gran;
            this.latch = latch;
        }

        @Override
        public Boolean call() throws Exception {
            TimerContext singleQueryTimerCtx = singleFetchTimer.time();
            try {
                MetricData data = reader.getDatapointsForRange(locator, range, gran);
                queryResults.put(locator, data);
                return true;
            } catch (Exception ex) {
                log.error("Exception reading metrics for locator " + locator, ex);
                failedReads.incrementAndGet();
                return false;
            } finally {
                singleQueryTimerCtx.stop();
                latch.countDown();
            }
        }
    }
}
