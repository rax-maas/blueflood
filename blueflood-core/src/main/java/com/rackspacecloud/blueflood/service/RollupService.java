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

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.tools.jmx.JmxBooleanGauge;
import com.rackspacecloud.blueflood.utils.Util;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.util.JmxGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class RollupService implements Runnable, RollupServiceMBean {
    private static final Logger log = LoggerFactory.getLogger(RollupService.class);
    private static final long ROLLUP_DELAY_MILLIS = 1000 * 60 * 5; // 5 minutes.
    
    private final ScheduleContext context;
    private final ShardStateManager shardStateManager;
    private final Timer polltimer = Metrics.newTimer(RollupService.class, "Poll Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Meter rejectedSlotChecks = Metrics.newMeter(RollupService.class, "Rejected Slot Checks", "Rollups", TimeUnit.MINUTES);
    private final ThreadPoolExecutor locatorFetchExecutors;
    private final ThreadPoolExecutor rollupExecutors;
    private long pollerPeriod = Configuration.getIntegerProperty("SCHEDULE_POLL_PERIOD");
    private final long configRefreshInterval = Configuration.getIntegerProperty("CONFIG_REFRESH_PERIOD");
    private transient Thread thread;
    
    private long lastSlotCheckFinishedAt = 0L;
    
    private boolean active = true;
    private boolean keepingServerTime = true;
    
    private Gauge activeGauge;
    private Gauge inflightRollupGauge;
    private Gauge pollerPeriodGauge;
    private Gauge serverTimeGauge;
    private Gauge rollupConcurrencyGauge;
    private Gauge scheduledSlotCheckGauge;
    private Gauge secondsSinceLastSlotCheckGauge;
    private Gauge queuedRollupGauge;
    private Gauge slotCheckConcurrencyGauge;
    private Gauge recentlyScheduledShardGauge;
    private Gauge managedShardGauge;

    protected static final AtomicLong lastRollupTime = new AtomicLong(System.currentTimeMillis());
    private static final Gauge<Long> timeSinceLastRollupGauge = Metrics.newGauge(RollupService.class, "Milliseconds Since Last Rollup", new Gauge<Long>() {
        @Override
        public Long value() {
            return System.currentTimeMillis() - lastRollupTime.get();
        }
    });

    public RollupService(ScheduleContext context) {
        this.context = context;
        this.shardStateManager = context.getShardStateManager();
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format("com.rackspacecloud.blueflood.service:type=%s", getClass().getSimpleName());
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            activeGauge = Metrics.newGauge(RollupService.class, "Active",
                    new JmxBooleanGauge(nameObj, "Active"));
            inflightRollupGauge = Metrics.newGauge(RollupService.class, "In Flight Rollup Count",
                    new JmxGauge(nameObj, "InFlightRollupCount"));
            pollerPeriodGauge = Metrics.newGauge(RollupService.class, "Poller Period",
                    new JmxGauge(nameObj, "PollerPeriod"));
            queuedRollupGauge = Metrics.newGauge(RollupService.class, "Queued Rollup Count",
                    new JmxGauge(nameObj, "QueuedRollupCount"));
            rollupConcurrencyGauge = Metrics.newGauge(RollupService.class, "Rollup Concurrency",
                    new JmxGauge(nameObj, "RollupConcurrency"));
            scheduledSlotCheckGauge = Metrics.newGauge(RollupService.class, "Scheduled Slot Check",
                    new JmxGauge(nameObj, "ScheduledSlotCheckCount"));
            secondsSinceLastSlotCheckGauge = Metrics.newGauge(RollupService.class, "Seconds Since Last Slot Check",
                    new JmxGauge(nameObj, "SecondsSinceLastSlotCheck"));
            pollerPeriodGauge = Metrics.newGauge(RollupService.class, "Poller Period",
                    new JmxGauge(nameObj, "PollerPeriod"));
            serverTimeGauge = Metrics.newGauge(RollupService.class, "Server Time",
                    new JmxGauge(nameObj, "ServerTime"));
            slotCheckConcurrencyGauge = Metrics.newGauge(RollupService.class, "Slot Check Concurrency",
                    new JmxGauge(nameObj, "SlotCheckConcurrency"));
            recentlyScheduledShardGauge = Metrics.newGauge(RollupService.class, "Recently Scheduled Shards",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return getRecentlyScheduledShards().size();
                        }
                    });
            managedShardGauge = Metrics.newGauge(RollupService.class, "Managed Shards",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return getManagedShards().size();
                        }
                    });
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + getClass().getSimpleName(), exc);
        }
        
        // NOTE: higher locatorFetchConcurrency means that the queue used in rollupExecutors needs to be correspondingly
        // higher.
        final int locatorFetchConcurrency = 2;
        locatorFetchExecutors = new ThreadPoolExecutor (
            locatorFetchConcurrency, locatorFetchConcurrency,
            30, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(locatorFetchConcurrency * 5),
            Executors.defaultThreadFactory(),
            new RejectedExecutionHandler() {
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    // in this case, we want to throw a RejectedExecutionException so that the slot can be removed
                    // from the running queue.
                    throw new RejectedExecutionException("Threadpool is saturated. unable to service this slot.");
                }
            }
        ) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                lastSlotCheckFinishedAt = RollupService.this.context.getCurrentTimeMillis();
                super.afterExecute(r, t);
            }
        };
        
        // unbounded work queue.
        final BlockingQueue<Runnable> rollupQueue = new LinkedBlockingQueue<Runnable>();
        rollupExecutors = new ThreadPoolExecutor ( 
            Configuration.getIntegerProperty("MAX_ROLLUP_THREADS"), 
            Configuration.getIntegerProperty("MAX_ROLLUP_THREADS"), 
            30, TimeUnit.SECONDS,
            rollupQueue,
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
    
    public void forcePoll() {
        thread.interrupt();
    }
    
    final void poll() {
        TimerContext timer = polltimer.time();
        // schedule for rollup anything that has not been updated in ROLLUP_DELAY_SECS
        context.scheduleSlotsOlderThan(ROLLUP_DELAY_MILLIS);
        timer.stop();
    }
    
    public void run() {
        thread = Thread.currentThread();

        while (true) {
            long startRun = System.currentTimeMillis();
            
            poll();
            
            // if there are schedules slots, run what we can.
            boolean rejected = false;
            while (context.hasScheduled() && !rejected && active) {
                final String slotKey = context.getNextScheduled(); 
                try {
                    log.debug("Scheduling slotKey {} @ {}", slotKey, context.getCurrentTimeMillis());
                    locatorFetchExecutors.execute(new LocatorFetchRunnable(context, slotKey, rollupExecutors));
                } catch (RejectedExecutionException ex) {
                    // puts it back at the top of the list of scheduled slots.  When this happens it means that 
                    // there is too much rollup work to do. if the CPU cores are not tapped out, it means you don't
                    // have enough threads allocated to processing rollups or slot checks.
                    rejectedSlotChecks.mark();
                    context.pushBackToScheduled(slotKey);
                    rejected = true;
                }
            }
            long endRun = System.currentTimeMillis();
            if (endRun - startRun > pollerPeriod)
                log.error("It took longer than {} to poll for rollups.", pollerPeriod);
            else
                try { 
                    thread.sleep(Math.max(0, pollerPeriod - endRun + startRun));
                } catch (Exception ex) {
                    log.debug("RollupService poller woke up");
                }
        }
    }

    //
    // JMX exposure
    //


    // set the server time in millis.
    public synchronized void setServerTime(long millis) {
        log.info("Manually setting server time to {}  {}", millis, new java.util.Date(millis));
        context.setCurrentTimeMillis(millis); 
    }

    // get the server time in seconds.
    public synchronized long getServerTime() { return context.getCurrentTimeMillis(); }

    public synchronized void setKeepingServerTime(boolean b) { keepingServerTime = b; }

    public synchronized boolean getKeepingServerTime() { return keepingServerTime; }
    
    public synchronized void setPollerPeriod(long l) {
        // todo: alter the design so that you don't have to keep a thread reference around. one way to do this is to
        // override the function in the caller (where the thread is accessible).
        pollerPeriod = l; 
        if (thread != null)
            thread.interrupt();
    }
    
    public synchronized long getPollerPeriod() { return pollerPeriod; }
    
    public synchronized int getScheduledSlotCheckCount() { return context.getScheduledCount(); }

    public synchronized int getSecondsSinceLastSlotCheck() {
        return (int)((context.getCurrentTimeMillis() - lastSlotCheckFinishedAt) / 1000);
    }

    public synchronized int getSlotCheckConcurrency() {
        return locatorFetchExecutors.getMaximumPoolSize();
    }

    public synchronized void setSlotCheckConcurrency(int i) {
        locatorFetchExecutors.setCorePoolSize(i);
        locatorFetchExecutors.setMaximumPoolSize(i);
    }

    public synchronized int getRollupConcurrency() {
        return rollupExecutors.getMaximumPoolSize();
    }

    public synchronized void setRollupConcurrency(int i) {
        rollupExecutors.setCorePoolSize(i);
        rollupExecutors.setMaximumPoolSize(i);
    }

    public synchronized int getQueuedRollupCount() { return rollupExecutors.getQueue().size(); }
    public synchronized int getInFlightRollupCount() { return rollupExecutors.getActiveCount(); }

    public synchronized boolean getActive() { return active; }
    
    public synchronized void setActive(boolean b) { 
        active = b;
        if (active && thread != null)
            thread.interrupt();
    }

    /**
     * Add a shard to be managed (via JMX)
     *
     * @param shard shard to be added
     */
    public void addShard(Integer shard) {
        if (!shardStateManager.getManagedShards().contains(shard))
            context.addShard(shard);
    }

    /**
     * Remove a shard from being managed (via JMX)
     *
     * @param shard shard to be removed
     */
    public void removeShard(Integer shard) {
        if (shardStateManager.getManagedShards().contains(shard))
            context.removeShard(shard);
    }

    /**
     * Get list of managed shards (via JMX)
     *
     * @return list of managed shards (unmodifiable collection)
     */
    public Collection<Integer> getManagedShards() {
        return new TreeSet<Integer>(shardStateManager.getManagedShards());
    }

    public synchronized Collection<Integer> getRecentlyScheduledShards() {
        // note: already sorted when it comes from the context.
        return context.getRecentlyScheduledShards();
    }

    public synchronized Collection<String> getMetricsState(int shard, int slot) {
        final List<String> results = new ArrayList<String>();

        for (Granularity g : Granularity.rollupGranularities()) {
            final Map<Integer, UpdateStamp> stateTimestamps = context.getSlotStamps(g, shard);
            if (stateTimestamps == null) {
                continue;
            }
            final UpdateStamp stamp = stateTimestamps.get(slot);
            if (stamp != null) {
                results.add(Util.formatStateColumnName(g, slot, stamp.getState().code()) + ": " + stamp.getTimestamp());
            }
        }

        return results;
    }

    public synchronized Collection<String> getOldestUnrolledSlotPerGranularity(int shard) {
        final Set<String> results = new HashSet<String>();

        for (Granularity g : Granularity.rollupGranularities()) {
            final Map<Integer, UpdateStamp> stateTimestamps = context.getSlotStamps(g, shard);
            if (stateTimestamps == null || stateTimestamps.isEmpty()) {
                continue;
            }

            // Iterate through the map of slot to UpdateStamp and find the oldest one
            long min = System.currentTimeMillis(); String minSlot = ""; boolean add = false;
            for (Map.Entry<Integer, UpdateStamp> entry : stateTimestamps.entrySet()) {
                final UpdateStamp stamp = entry.getValue();
                if (stamp.getState() != UpdateStamp.State.Rolled && stamp.getTimestamp() < min) {
                    min = stamp.getTimestamp();
                    minSlot = Util.formatStateColumnName(g, entry.getKey(), stamp.getState().code());
                    add = true;
                }
            }

            if (add) {
                results.add(minSlot + ": " + min);
            }
        }

        return results;
    }
}
