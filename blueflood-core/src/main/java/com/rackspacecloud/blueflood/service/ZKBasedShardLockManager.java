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

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.google.common.base.Ticker;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
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
import java.util.concurrent.atomic.AtomicInteger;

class ZKBasedShardLockManager implements ConnectionStateListener, ShardLockManager, ZKBasedShardLockManagerMBean {
    private Logger log = LoggerFactory.getLogger(ZKBasedShardLockManager.class);
    private static AtomicInteger UNIQUE_IDENTIFIER = new AtomicInteger(0);

    private final Random rand = new Random(System.currentTimeMillis());
    private final int id = UNIQUE_IDENTIFIER.getAndIncrement();
    private CuratorFramework client;
    private final String ZK_NAMESPACE = "locks/blueflood";
    private final String LOCK_QUALIFIER = "/shards";
    private final long ZK_SESSION_TIMEOUT_MS = new TimeValue(120L, TimeUnit.SECONDS).toMillis();
    private final long ZK_CONN_TIMEOUT_MS = new TimeValue(5L, TimeUnit.SECONDS).toMillis();
    private final long ZK_RETRY_INTERVAL = new TimeValue(50L, TimeUnit.MILLISECONDS).toMillis();
    private final int ZK_MAX_RETRIES = 2;
    private final TimeValue ZK_LOCK_TIMEOUT = new TimeValue(1L, TimeUnit.SECONDS);
    private final ConcurrentHashMap<Integer, Lock> locks; // shard to lock objects
    private boolean connected = false;
    private final Ticker ticker = Ticker.systemTicker();

    private TimeValue minLockHoldTime = new TimeValue(20, TimeUnit.MINUTES);
    private TimeValue lockDisinterestedTime = new TimeValue(1, TimeUnit.MINUTES);
    private TimeValue shardLockScavengeInterval = new TimeValue(2L, TimeUnit.MINUTES);
    private java.util.Timer lockScavenger = null;
    private long lastScavengedAt = System.currentTimeMillis();
    private int maxLocksToAcquirePerCycle;
    private int defaultMaxLocksToAcquirePerCycle;
    private AtomicInteger locksAcquiredThisCycle = new AtomicInteger(0);
    private Gauge lockDisinterestedTimeMillisGauge;
    private Gauge minLockHoldTimeMillisGauge;
    private Gauge secondsSinceLastScavengeGauge;
    private Gauge zkConnectionStatusGauge;
    private Gauge heldShardGauge;
    private Gauge unheldShardGauge;
    private Gauge errorShardGauge;

    // thread that does the lock work.
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(1000), new ThreadFactory() {
        public Thread newThread(Runnable r) {
            return new Thread(r, "ZK Lock Worker " + id);
        }
    });

    private final Meter lockAcquisitionFailure = Metrics.newMeter(ZKBasedShardLockManager.class, "Lock acquisition failures", "ZK", TimeUnit.MINUTES);
    private final Timer lockAcquisitionTimer = Metrics.newTimer(ZKBasedShardLockManager.class, "Lock acquisition timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Meter lockErrors = Metrics.newMeter(ZKBasedShardLockManager.class, "Lock errors", "ZK", TimeUnit.MINUTES);

    ZKBasedShardLockManager(String zookeeperCluster, Set<Integer> managedShards) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format("com.rackspacecloud.blueflood.service:type=%s", getClass().getSimpleName() + (id == 0 ? "" : id));
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            lockDisinterestedTimeMillisGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Lock Disinterested Time Millis",
                    new JmxGauge(nameObj, "LockDisinterestedTimeMillis"));
            minLockHoldTimeMillisGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Min Lock Hold Time Millis",
                    new JmxGauge(nameObj, "MinLockHoldTimeMillis"));
            secondsSinceLastScavengeGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Seconds Since Last Scavenge",
                    new JmxGauge(nameObj, "SecondsSinceLastScavenge"));
            zkConnectionStatusGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Zk Connection Status",
                    new JmxGauge(nameObj, "ZkConnectionStatus") {
                        @Override
                        public Object value() {
                            Object val = super.value();
                            if (val.equals("connected")) {
                                return 1;
                            }
                            return 0;
                        }
                    });
            heldShardGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Held Shards",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return getHeldShards().size();
                        }
                    });
            unheldShardGauge = Metrics.newGauge(ZKBasedShardLockManager.class, "Unheld Shards",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return getUnheldShards().size();
                        }
                    });
            errorShardGauge =  Metrics.newGauge(ZKBasedShardLockManager.class, "Error Shards",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return getErrorShards().size();
                        }
                    });
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + getClass().getSimpleName(), exc);
        }

        this.locks = new ConcurrentHashMap<Integer, Lock>();
        this.client = null;
        RetryPolicy policy = new ExponentialBackoffRetry((int)ZK_RETRY_INTERVAL, ZK_MAX_RETRIES);

        this.client = CuratorFrameworkFactory.
                builder().namespace(ZK_NAMESPACE)
                .connectString(zookeeperCluster)
                .sessionTimeoutMs((int) ZK_SESSION_TIMEOUT_MS)
                .connectionTimeoutMs((int) ZK_CONN_TIMEOUT_MS)
                .retryPolicy(policy).build();
        this.client.getConnectionStateListenable().addListener(this);  // register our listener
        for (int shard : managedShards)
            addShard(shard);
        this.client.start();

        Configuration config = Configuration.getInstance();
        this.minLockHoldTime = new TimeValue(config.getLongProperty("SHARD_LOCK_HOLD_PERIOD_MS"), TimeUnit.MILLISECONDS);
        this.lockDisinterestedTime = new TimeValue(config.getLongProperty("SHARD_LOCK_DISINTERESTED_PERIOD_MS"), TimeUnit.MILLISECONDS);
        this.shardLockScavengeInterval = new TimeValue(config.getLongProperty("SHARD_LOCK_SCAVENGE_INTERVAL_MS"),
                TimeUnit.MILLISECONDS);
        this.lockScavenger = new java.util.Timer("Lock scavenger " + (id != 0 ? id : ""), true);
        this.defaultMaxLocksToAcquirePerCycle = config.getIntegerProperty("MAX_ZK_LOCKS_TO_ACQUIRE_PER_CYCLE");

        waitForZKConnections();
        // attempt all locks. needs to be called before the scavenger is instantiated.
        prefetchLocksAndScheduleLocksScavenging(managedShards);
    }

    private void waitForZKConnections() {
        for (int i = 0; i < 5; i++) {
            if (!connected) {
                log.debug("Waiting for connect");
                try { Thread.sleep(1000); } catch (Exception ex) {}
            }
        }
    }

    // Only called from the constructor; Iterates over shards trying to acquire locks.
    private void prefetchLocksAndScheduleLocksScavenging(final Collection<Integer> managedShards) {
        // Try to achieve each lock. This will give us a good initial state where each lock is either held or unheld.
        // The purpose is to avoid a situation at startup where a node assumes it can schedule anything (until a lock
        // is held).

        if (!connected) {
            log.warn("Cannot connect to Zookeeper; will not perform initial lock acquisition");
            for (Lock lock : locks.values())
                lock.connectionLost();
        } else {
            log.info("Pre-fetching zookeeper locks for shards");
            boolean isManagingAllShards = managedShards.size() >= Constants.NUMBER_OF_SHARDS;
            int maxLocksToPrefetch = managedShards.size()/2 + 1;

            if (isManagingAllShards) {
                maxLocksToPrefetch = managedShards.size();
            }

            List<Integer> shards = new ArrayList<Integer>(managedShards);
            Collections.shuffle(shards);

            int locksObtained = 0;
            for (int shard : shards) {
                try {
                    log.debug("Initial lock attempt for " + shard);
                    final Lock lock = locks.get(shard);
                    worker.submit(lock.acquirer()).get();

                    if (lock.isHeld() && ++locksObtained >= maxLocksToPrefetch) {
                        break;
                    }
                } catch (InterruptedException ex) {
                    log.warn("Thread exception while acquiring initial locks: " + ex.getMessage(), ex);
                } catch (ExecutionException ex) {
                    log.error("Problem acquiring lock " + shard + " " + ex.getCause().getMessage(), ex.getCause());
                }
            }
            log.info("Finished pre-fetching zookeeper locks");
        }

        // initial locks are set. safe to start the scavenger now.
        this.lockScavenger.schedule(new TimerTask() {
            @Override
            public void run() {
                scavengeLocks();
            }
        }, 120000, this.shardLockScavengeInterval.toMillis()); // wait 2 min, then check every shardLockScavengeInterval.
    }

    public boolean canWork(int shard) {
        return locks.containsKey(shard) && locks.get(shard).canWork();
    }

    private String getLockId(int shard) {
        return  LOCK_QUALIFIER + "/" + shard;
    }

    // This is called when connection to zookeeper is lost
    private void handleZookeeperConnectionFailed() {
        // It is okay for us to proceed with the work we already scheduled (either running or in scheduled queue)
        // for slots. We would duplicate work just for those slots which is fine.
        log.info("Force release all locks as zookeeper connection is lost");
        for (Lock lock : locks.values())
            lock.connectionLost();
    }

    private void scavengeLocks() {
        locksAcquiredThisCycle.set(0);
        maxLocksToAcquirePerCycle = defaultMaxLocksToAcquirePerCycle;
        final Integer[] shards = locks.keySet().toArray(new Integer[]{});

        // Linear scan to figure out how many locks are held.
        int locksHeld = 0;
        for (int shard : shards) {
            if (locks.get(shard).isHeld()) {
                locksHeld++;
            }
        }

        // if the number of locks held is less than 50% of the number of managed shards,
        // be aggressive and acquire more locks
        if (locksHeld <= locks.size()/2 + 1) {
            maxLocksToAcquirePerCycle = locks.size()/2 + 1;
        }

        for (int shard : shards) {
            locks.get(shard).performMaintenance();
        }
        lastScavengedAt = nowMillis();
    }

    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        log.info("Connection to Zookeeper toggled to state " + connectionState.toString());
        connected = connectionState == ConnectionState.CONNECTED || connectionState == ConnectionState.RECONNECTED;
        if (connectionState == ConnectionState.LOST) {
            log.error("Connection to Zookeeper toggled to state " + connectionState.toString());
            this.handleZookeeperConnectionFailed();
        } else if (connectionState == ConnectionState.RECONNECTED) {
            log.info("Reconnected to zookeeper, forcing lock scavenge");
            forceLockScavenge();
        } else {
            log.info("Connection to Zookeeper toggled to state " + connectionState.toString());
        }
    }

    public synchronized void addShard(int shard) {
        if (locks.containsKey(shard))
            return;
        this.locks.put(shard, new Lock(shard));
    }

    public synchronized void removeShard(int shard) {
        Lock lock = locks.remove(shard);
        if (lock != null)
            lock.release();
    }

    public boolean isConnected() {
        return connected && client != null && isCuratorStarted() && client.getZookeeperClient().isConnected();
    }

    private boolean isCuratorStarted() {
        return client.getState() == CuratorFrameworkState.STARTED;
    }

    //
    // unsafe methods for testing.
    //

    public void waitForQuiesceUnsafe() {
        while (worker.getActiveCount() != 0 || worker.getQueue().size() != 0) {
            if (log.isTraceEnabled())
                log.trace("Waiting for quiesce");
            try { Thread.sleep(100); } catch (InterruptedException ignore) {}
        }
        // this time out nees to be longer than ZK_LOCK_TIMEOUT for valid tests.
        try { Thread.sleep(1500); } catch (InterruptedException ignore) {}
    }

    public boolean holdsLockUnsafe(int shard) {
        return locks.containsKey(shard) && locks.get(shard).isHeldZk();
    }

    public boolean releaseLockUnsafe(int shard) throws Exception {
        return worker.submit(locks.get(shard).releaser()).get();
    }

    public void shutdownUnsafe() throws Exception {
        for (Lock lock : locks.values())
            worker.submit(lock.releaser()).get();
        client.close();
    }

    private long nowMillis() { return ticker.read() / 1000000; }

    //
    // JMX
    //

    public synchronized Collection<Integer> getHeldShards() {
        SortedSet<Integer> held = new TreeSet<Integer>();
        for (Lock lock : locks.values()) {
            if (lock.isHeld()) {
                held.add(lock.getShard());
            }
        }
        return held;
    }

    public synchronized Collection<Integer> getUnheldShards() {
        SortedSet<Integer> unheld = new TreeSet<Integer>();
        for (Lock lock : locks.values()) {
            if (lock.isUnheld()) {
                unheld.add(lock.getShard());
            }
        }
        return unheld;
    }

    public synchronized Collection<Integer> getErrorShards() {
        SortedSet<Integer> errorShards = new TreeSet<Integer>();
        for (Lock lock : locks.values()) {
            if (lock.getLockState() == LockState.ERROR) {
                errorShards.add(lock.getShard());
            }
        }
        return errorShards;
    }

    public synchronized void forceLockScavenge() {
        scavengeLocks();
    }

    public synchronized String getZkConnectionStatus() {
        if (client == null)
            return "null";
        else if (!isCuratorStarted())
            return "not started";
        else if (client.getZookeeperClient().isConnected())
            return "connected";
        else
            return "not connected";
    }

    public synchronized boolean release(int shard) {
        if (locks.containsKey(shard)) {
            try {
                return worker.submit(locks.get(shard).releaser()).get();
            } catch (InterruptedException ex) {
                log.error("Thread error: "+ ex.getMessage(), ex);
                return false;
            } catch (ExecutionException ex) {
                log.error("Release error: " + ex.getCause().getMessage(), ex.getCause());
                return false;
            }
        }
        return false;
    }

    public synchronized boolean acquire(int shard) {
        if (locks.containsKey(shard)) {
            try {
                return worker.submit(locks.get(shard).acquirer()).get();
            } catch (InterruptedException ex) {
                log.error("Thread error: "+ ex.getMessage(), ex);
                return false;
            } catch (ExecutionException ex) {
                log.error("Release error: " + ex.getCause().getMessage(), ex.getCause());
                return false;
            }
        }
        return false;
    }

    public synchronized long getMinLockHoldTimeMillis() { return minLockHoldTime.toMillis(); }
    public synchronized void setMinLockHoldTimeMillis(long millis) { minLockHoldTime = new TimeValue(millis, TimeUnit.MILLISECONDS); }
    public synchronized long getLockDisinterestedTimeMillis() { return lockDisinterestedTime.toMillis(); }
    public synchronized void setLockDisinterestedTimeMillis(long millis) { lockDisinterestedTime = new TimeValue(millis, TimeUnit.MILLISECONDS); }
    public synchronized long getSecondsSinceLastScavenge() { return ((nowMillis() - lastScavengedAt) / 1000); }

    //
    // Helper classes
    //


    enum LockState {
        UNKNOWN,
        ACQUIRED,
        ACQUIRE_FAILED,
        ERROR,
        DISINTERESTED;
    }

    class Lock {
        private final int shard;
        private LockState state = LockState.UNKNOWN;
        private InterProcessMutex mutex = null;
        private long stateChanged = ticker.read() / 1000000;
        private boolean isAcquiring = false;
        private boolean isReleasing = false;

        Lock(int shard) {
            this.shard = shard;
            checkMutex();
        }

        synchronized void checkMutex() {
            if (client == null) {
                state = LockState.ERROR;
                stateChanged = nowMillis();
            } else if (mutex == null) {
                mutex = new InterProcessMutex(client, getLockId(shard));
            }
        }

        int getShard() { return shard; }
        boolean isHeld() { return mutex != null && state == LockState.ACQUIRED; }
        boolean isUnheld() { return mutex != null && state == LockState.ACQUIRE_FAILED; }

        boolean isHeldZk() {
            return mutex != null && mutex.isAcquiredInThisProcess();
        }

        void performMaintenance() {
            updateLockState();

            long now = nowMillis();
            // determine if we should acquire or release.
            if (state == LockState.UNKNOWN && locksAcquiredThisCycle.get() < maxLocksToAcquirePerCycle) {
                acquire();

                if (isHeld()) {
                    locksAcquiredThisCycle.incrementAndGet();
                }
            } else if (state == LockState.ACQUIRED && now - stateChanged > minLockHoldTime.toMillis()) {
                // maybe release.
                float chance = (float)(now - stateChanged - minLockHoldTime.toMillis()) / (float)(Math.max(1, minLockHoldTime.toMillis()));
                float r = rand.nextFloat();
                if (log.isTraceEnabled())
                    log.trace(String.format("Will release %s if, %f < %f", shard, r, chance));
                if (r < chance)
                    release();
            } else if (state == LockState.ERROR && now - stateChanged > minLockHoldTime.toMillis()) {
                log.error("Lock state hasn't toggled from ERROR for " + minLockHoldTime.toString()
                        + "; Client connection status: " + connected);
            }
        }

        synchronized void updateLockState() {
            boolean toUnk = false;
            long now = nowMillis();

            if (state == LockState.DISINTERESTED && now - stateChanged > lockDisinterestedTime.toMillis())
                toUnk = true;
            else if (state == LockState.ERROR && connected)
                toUnk = true;
            else if (state == LockState.ACQUIRE_FAILED && now - stateChanged > lockDisinterestedTime.toMillis())
                toUnk = true;

            if (toUnk) {
                state = LockState.UNKNOWN;
                stateChanged = now;
            }
        }

        synchronized LockState getLockState() {
            return state;
        }

        synchronized boolean canWork() {
            return state == LockState.ACQUIRED || state == LockState.ERROR;
        }

        synchronized void connectionLost() {
            // assume client = null.
            state = LockState.ERROR;
            stateChanged = nowMillis();
            mutex = null;
        }

        synchronized void acquire() {
            if (isAcquiring || isReleasing) return;
            isAcquiring = true;
            try {
                log.debug("Acquiring lock for " + shard);
                worker.execute(new FutureTask<Boolean>(acquirer()));
            } catch (RejectedExecutionException ex) {
                log.warn(String.format("Rejected lock execution: active:%d queue:%d shard:%d", worker.getActiveCount(), worker.getQueue().size(), shard));
            }
        }

        synchronized void release() {
            if (isAcquiring || isReleasing) return;
            isReleasing = true;
            try {
                log.debug("Releasing lock for " + shard);
                worker.execute(new FutureTask<Boolean>(releaser()));
            } catch (RejectedExecutionException ex) {
                log.warn(String.format("Rejected lock execution: active:%d queue:%d shard:%d", worker.getActiveCount(), worker.getQueue().size(), shard));
            }
        }

        synchronized Callable<Boolean> acquirer() {
            return new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    checkMutex();
                    if (client == null || !connected || mutex == null) {
                        state = LockState.ERROR;
                        stateChanged = nowMillis();
                        return false;
                    } else if (state == LockState.ACQUIRED)
                        return true;
                    else if (state == LockState.ACQUIRE_FAILED)
                        return false;
                    else {
                        log.debug("Trying ZK lock for " + shard);
                        TimerContext ctx = lockAcquisitionTimer.time();
                        if (mutex.isAcquiredInThisProcess()) {
                            if (log.isTraceEnabled())
                                log.trace("Lock already acquired for " + shard);
                            ctx.stop();
                            return true;
                        } else {
                            try {
                                boolean acquired = mutex.acquire(ZK_LOCK_TIMEOUT.getValue(), ZK_LOCK_TIMEOUT.getUnit());
                                if (acquired) {
                                    state = LockState.ACQUIRED;
                                    stateChanged = nowMillis();
                                    log.debug("Acquired ZK lock for " + shard);
                                } else {
                                    state = LockState.ACQUIRE_FAILED;
                                    stateChanged = nowMillis();
                                    lockAcquisitionFailure.mark();
                                    log.debug("Acquire ZK failed for " + shard);
                                }
                                return acquired;
                            } catch (Exception ex) {
                                log.debug("Exception on ZK acquire for " + shard);
                                log.warn(ex.getMessage(), ex);
                                lockErrors.mark();
                                state = LockState.ERROR;
                                return true;
                            } finally {
                                isAcquiring = false;
                                ctx.stop();
                            }
                        }
                    }
                }
            };
        }

        synchronized Callable<Boolean> releaser() {
            return new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    checkMutex();
                    boolean result = false;
                    if (client == null || !connected || mutex == null) {
                        state = LockState.ERROR;
                        stateChanged = nowMillis();
                        result = true;
                    } else if (state != LockState.ACQUIRED)
                        result = false;
                    else if (mutex.isAcquiredInThisProcess()) {
                        log.debug("Releasing lock for shard " + shard);
                        mutex.release();
                        state = LockState.DISINTERESTED;
                        stateChanged = nowMillis();
                        result = true;
                    } else {
                        log.error("Held lock not held by this process? " + shard);
                        state = LockState.UNKNOWN;
                        stateChanged = nowMillis();
                        result =  true;
                    }
                    isReleasing = false;
                    return result;
                }
            };
        }
    }
}