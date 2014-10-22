/*
 * Copyright 2014 Rackspace
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
package com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers;

import com.rackspacecloud.blueflood.CloudFilesBackfiller.service.BackFillerConfig;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.RollupBatchWriter;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class RollupGenerator implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RollupGenerator.class);
    public static final ThreadPoolExecutor rollupExecutors = new ThreadPoolBuilder().withName("CFBackFillerRollupThreadPool")
            .withMaxPoolSize(Configuration.getInstance().getIntegerProperty(BackFillerConfig.ROLLUP_THREADS))
            .withCorePoolSize(Configuration.getInstance().getIntegerProperty(BackFillerConfig.ROLLUP_THREADS))
            .withUnboundedQueue()
            .build();
    private static final ThreadPoolExecutor batchWriterPool = new ThreadPoolBuilder().withName("CFBackfillerRollupWriterThreadPool")
            .withMaxPoolSize(Configuration.getInstance().getIntegerProperty(BackFillerConfig.BATCH_WRITER_THREADS))
            .withCorePoolSize(Configuration.getInstance().getIntegerProperty(BackFillerConfig.BATCH_WRITER_THREADS))
            .withUnboundedQueue()
            .build();

    @Override
    public void run() {
        boolean running = true;
        while (running) {
            Map<Range,ConcurrentHashMap<Locator,Points>> dataToBeRolled = BuildStore.getEligibleData();
            try {
                if (dataToBeRolled != null && !dataToBeRolled.isEmpty()) {
                    final RollupBatchWriter batchWriter = new RollupBatchWriter(batchWriterPool, null);
                    Set<Range> ranges = dataToBeRolled.keySet();
                    for (Range range : ranges) {
                        // Remove the range from applicable range, so that we do not merge any more data for that range.
                        BuildStore.rangesStillApplicable.remove(range);
                        log.info("Removed range {} from applicable ranges", range);

                        Set<Locator> locators = dataToBeRolled.get(range).keySet();
                        for (Locator locator : locators) {
                            rollupExecutors.submit(new RollupValidatorAndComputer(locator, range, dataToBeRolled.get(range).get(locator), batchWriter));
                        }
                        //This will remove the entry from the backing map in buildstore as well. This is the operation that clears memory and prevents it from blowing up!
                        dataToBeRolled.remove(range);
                        log.info("Removed range {} from buildstore", range);
                        // Drain the remaining rollups
                        batchWriter.drainBatch();
                    }
                } else {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.info("Rollup Generator Thread interrupted");
                        running = false;
                    }
                }
            } catch (Throwable e) {
                log.error("Exception encountered while calculating rollups", e);
                throw new RuntimeException(e);
            }
        }
    }
}
