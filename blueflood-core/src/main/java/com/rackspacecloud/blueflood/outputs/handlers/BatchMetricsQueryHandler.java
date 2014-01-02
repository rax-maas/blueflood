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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.BatchMetricsQuery;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
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
    private Map<Locator, MetricData> queryResults;
    private static final Timer queryTimer = Metrics.timer(Instrumentation.class,
            "Batched Metrics Query Duration");
    private static final Meter exceededQueryTimeout = Metrics.meter(Instrumentation.class,
            "Batched Metrics Query Duration Exceeded Timeout");
    private static final Histogram queriesSizeHist = Metrics.histogram(Instrumentation.class,
            "Total queries");

    public BatchMetricsQueryHandler(ThreadPoolExecutor executor, AstyanaxReader reader) {
        this.executor = executor;
        this.reader = reader;
        this.queryResults = null;
    }

    public Map<Locator, MetricData> execute(final BatchMetricsQuery query, TimeValue queryTimeout)
            throws Exception {
        final CountDownLatch shortLatch = new CountDownLatch(1);
        final Timer.Context queryTimerCtx = queryTimer.time();

        Future<Boolean> result = executor.submit(
            new MetricFetchCallable(query, shortLatch)
        );


        // Wait until timeout happens or you got all the results
        shortLatch.await(queryTimeout.getValue(), queryTimeout.getUnit());
        queryTimerCtx.stop();

        boolean inProgress = (shortLatch.getCount() > 0);

        if (inProgress) {
            if (!result.isDone()) {
                result.cancel(true);
            }

            log.warn("Interrupted batch fetch for metrics. Exceeded timeout of " + queryTimeout.toString());
            exceededQueryTimeout.mark();
            executor.purge();
        }
        queriesSizeHist.update(query.getLocators().size());

        return queryResults;
    }


    public class MetricFetchCallable implements Callable<Boolean> {
        private final BatchMetricsQuery query;
        private final CountDownLatch latch;

        public MetricFetchCallable(BatchMetricsQuery query, CountDownLatch latch) {
            this.query = query;
            this.latch = latch;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                queryResults= reader.getDatapointsForRange(query.getLocators(), query.getRange(), query.getGranularity());
                return true;
            } catch (Exception ex) {
                log.error("Exception reading batch of metrics ", ex);
                return false;
            } finally {
                latch.countDown();
            }
        }
    }
}
