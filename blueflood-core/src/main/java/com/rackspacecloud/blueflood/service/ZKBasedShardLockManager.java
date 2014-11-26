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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxAttributeGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.concurrent.InstrumentedThreadPoolExecutor;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class ZKBasedShardLockManager implements ConnectionStateListener, ShardLockManager, ZKBasedShardLockManagerMBean {
    private static final Logger log = LoggerFactory.getLogger(ZKBasedShardLockManager.class);
    private static final AtomicInteger UNIQUE_IDENTIFIER = new AtomicInteger(0);

    private final Random rand = new Random(System.currentTimeMillis());
    /** Unique identifier for this object, used to identify the MBean. */
    private final int id = UNIQUE_IDENTIFIER.getAndIncrement();
    /** Zookeeper client. */
    private final CuratorFramework client;
    private final String ZK_NAMESPACE = "locks/blueflood";
    private final String LOCK_QUALIFIER = "/shards";
    private final long ZK_SESSION_TIMEOUT_MS = new TimeValue(120L, TimeUnit.SECONDS).toMillis();
    private final long ZK_CONN_TIMEOUT_MS = new TimeValue(5L, TimeUnit.SECONDS).toMillis();
    private final long ZK_RETRY_INTERVAL = new TimeValue(50L, TimeUnit.MILLISECONDS).toMillis();
    private final int ZK_MAX_RETRIES = 2;
    private final TimeValue ZK_LOCK_TIMEOUT = new TimeValue(1L, TimeUnit.SECONDS);
    private final ConcurrentHashMap<Integer, Lock> locks; // shard to lock objects
    private final TimeValue shardLockScavengeInterval;
    private final int defaultMaxLocksToAcquirePerCycle;
    private final Ticker ticker = Ticker.systemTicker();

    // modifiable properties.
    private TimeValue minLockHoldTime;
    private TimeValue lockDisinterestedTime;

    /** true if we're connected to zookeeper. */
    private volatile boolean connected = false;
    private volatile long lastScavengedAt = System.currentTimeMillis();

    /** Thread that performs the locking & releasing. */
    private final ThreadPoolExecutor lockWorker;

    private final ScheduledThreadPoolExecutor scavengerWorker = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "ZK Lock Worker " + id);
            t.setDaemon(true);
            return t;
        }
    });

    private final Meter lockAcquisitionFailure = Metrics.meter(ZKBasedShardLockManager.class, "Lock acquisition failures");
    private final com.codahale.metrics.Timer lockAcquisitionTimer = Metrics.timer(ZKBasedShardLockManager.class, "Lock acquisition timer");
    private final Meter lockErrors = Metrics.meter(ZKBasedShardLockManager.class, "Lock errors");

    ZKBasedShardLockManager(String zookeeperCluster, Set<Integer> managedShards) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format("com.rackspacecloud.blueflood.service:type=%s", getClass().getSimpleName() + (id == 0 ? "" : id));
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
            registerMetrics(nameObj, Metrics.getRegistry());
        } catch (Exception exc) {
            log.error("Unable to register mbean for " + getClass().getSimpleName(), exc);
        }

        this.locks = new ConcurrentHashMap<Integer, Lock>();
        RetryPolicy policy = new ExponentialBackoffRetry((int)ZK_RETRY_INTERVAL, ZK_MAX_RETRIES);

        this.client = CuratorFrameworkFactory.
                builder().namespace(ZK_NAMESPACE)
                .connectString(zookeeperCluster)
                .sessionTimeoutMs((int) ZK_SESSION_TIMEOUT_MS)
                .connectionTimeoutMs((int) ZK_CONN_TIMEOUT_MS)
                .retryPolicy(policy).build();
        this.client.getConnectionStateListenable().addListener(this);  // register our listener
        this.client.start();

        Configuration config = Configuration.getInstance();
        for (int shard : managedShards) {
            addShard(shard);
        }
        this.minLockHoldTime = new TimeValue(config.getLongProperty(CoreConfig.SHARD_LOCK_HOLD_PERIOD_MS), TimeUnit.MILLISECONDS);
        this.lockDisinterestedTime = new TimeValue(config.getLongProperty(CoreConfig.SHARD_LOCK_DISINTERESTED_PERIOD_MS), TimeUnit.MILLISECONDS);
        this.shardLockScavengeInterval = new TimeValue(config.getLongProperty(CoreConfig.SHARD_LOCK_SCAVENGE_INTERVAL_MS),
                TimeUnit.MILLISECONDS);
        this.defaultMaxLocksToAcquirePerCycle = config.getIntegerProperty(CoreConfig.MAX_ZK_LOCKS_TO_ACQUIRE_PER_CYCLE);
        this.lockWorker = new ThreadPoolBuilder()
                .withCorePoolSize(1)
                .withMaxPoolSize(1)
                .withKeepAliveTime(new TimeValue(Long.MAX_VALUE, TimeUnit.DAYS))
                .withBoundedQueue(1000)
                .withName("ZkThreadPool")
                .build();
        InstrumentedThreadPoolExecutor.instrument(lockWorker, "ZkThreadPool");
    }

    /**
     * Registers the different zookeeper metrics.
     */
    private void registerMetrics(final ObjectName nameObj, MetricRegistry reg) {
        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Lock Disinterested Time Millis"),
                new JmxAttributeGauge(nameObj, "LockDisinterestedTimeMillis"));
        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Min Lock Hold Time Millis"),
                new JmxAttributeGauge(nameObj, "MinLockHoldTimeMillis"));
        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Seconds Since Last Scavenge"),
                new JmxAttributeGauge(nameObj, "SecondsSinceLastScavenge"));

        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Zk Connection Status"),
                new JmxAttributeGauge(nameObj, "ZkConnectionStatus") {
                    @Override
                    public Object getValue() {
                        Object val = super.getValue();
                        if (val.equals("connected")) {
                            return 1;
                        }
                        return 0;
                    }
                });
        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Held Shards"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return getHeldShards().size();
                    }
                });

        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Unheld Shards"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return getUnheldShards().size();
                    }
                });
        reg.register(MetricRegistry.name(ZKBasedShardLockManager.class, "Error Shards"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return getErrorShards().size();
                    }
                });
    }

    /**
     * Initialize the ZKBasedShardLockManager.
     * @param zkWaitTime Time to wait until zookeeper is up.
     */
    public void init(TimeValue zkWaitTime) {
        waitForZKConnections(zkWaitTime.toSeconds());
        prefetchLocks();
        scheduleScavenger();
    }

    /**
     * Waits until the zookeeper connection is available.
     * @param waitTimeSeconds
     */
    @VisibleForTesting boolean waitForZKConnections(long waitTimeSeconds) {
        for (int i = 0; i < waitTimeSeconds; i++) {
            if (connected) {
                return connected;
            }
            log.debug("Waiting for connect");
            try { Thread.sleep(1000); } catch (InterruptedException ex) {}
        }
        return connected;
    }

    /**
     * Only called from {@link #init(TimeValue)}.
     *
     * Try to achieve each lock. This will give us a good initial state where each lock is either held or unheld.
     * The purpose is to avoid a situation at startup where a node assumes it can schedule anything (until a lock
     * is held).
     */
    @VisibleForTesting void prefetchLocks() {

        if (!connected) {
            log.warn("Cannot connect to Zookeeper; will not perform initial lock acquisition");
            for (Lock lock : locks.values()) {
                lock.connectionLost();
            }
        } else {
            log.info("Pre-fetching zookeeper locks for shards");
            boolean isManagingAllShards = locks.size() >= Constants.NUMBER_OF_SHARDS;
            int maxLocksToPrefetch = moreThanHalf();

            if (isManagingAllShards) {
                maxLocksToPrefetch = Constants.NUMBER_OF_SHARDS;
            }

            List<Integer> shards = new ArrayList<Integer>(locks.keySet());
            Collections.shuffle(shards);

            int locksObtained = 0;
            for (int shard : shards) {
                try {
                    log.debug("Initial lock attempt for shard={}", shard);
                    final Lock lock = locks.get(shard);
                    lockWorker.submit(lock.acquirer()).get();

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
    }

    public void scheduleScavenger() {
        scavengerWorker.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scavengeLocks();
            }
        }, 30, shardLockScavengeInterval.toSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Determines whether a given shard can be worked on by this blueflood instance.
     */
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
        for (Lock lock : locks.values()) {
            lock.connectionLost();
        }
    }

    private void scavengeLocks() {
        log.debug("Starting scavengeLocks()");
        try {
            int locksAcquiredThisCycle = 0;
            int maxLocksToAcquirePerCycle = defaultMaxLocksToAcquirePerCycle;
            final Integer[] shards = locks.keySet().toArray(new Integer[]{});

            // Linear scan to figure out how many locks are held.
            int locksHeld = 0;
            for (int shard : shards) {
                if (locks.get(shard).isHeld()) {
                    locksHeld++;
                }
            }
            log.debug("Currently holding {} locks.", locksHeld);

            // if the number of locks held is less than 50% of the number of managed shards,
            // be aggressive and acquire more locks
            if (locksHeld <= moreThanHalf()) {
                maxLocksToAcquirePerCycle = moreThanHalf();
            }
            // shouldAttempt

            for (int shard : shards) {
                boolean shouldAttempt = locksAcquiredThisCycle < maxLocksToAcquirePerCycle;
                boolean isAcquired = locks.get(shard).performMaintenance(shouldAttempt);
                if (isAcquired) {
                    locksAcquiredThisCycle++;
                }
            }
            lastScavengedAt = nowMillis();
        } catch (RuntimeException e) {
            log.error("Error while scavengeLocks()", e);
        } finally {
            log.debug("Finishing scavengeLocks().");
        }
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
        if (locks.containsKey(shard)) {
            return;
        }
        this.locks.put(shard, new Lock(shard));
    }

    public synchronized void removeShard(int shard) {
        Lock lock = locks.remove(shard);
        if (lock != null) {
            lock.release();
        }
    }

    public boolean isConnected() {
        return connected && isCuratorStarted() && client.getZookeeperClient().isConnected();
    }

    /** Unsafe method for testing. */
    @VisibleForTesting
    void waitForQuiesceUnsafe() {
        while (lockWorker.getActiveCount() != 0 || lockWorker.getQueue().size() != 0) {
            if (log.isTraceEnabled())
                log.trace("Waiting for quiesce");
            try { Thread.sleep(100); } catch (InterruptedException ignore) {}
        }
        // this time out needs to be longer than ZK_LOCK_TIMEOUT for valid tests.
        try { Thread.sleep(2000); } catch (InterruptedException ignore) {}
    }

    @VisibleForTesting
    boolean holdsLockUnsafe(int shard) {
        return locks.containsKey(shard) && locks.get(shard).isHeldZk();
    }

    @VisibleForTesting
    boolean releaseLockUnsafe(int shard) throws Exception {
        return lockWorker.submit(locks.get(shard).releaser()).get();
    }

    @VisibleForTesting
    void shutdownUnsafe() throws Exception {
        for (Lock lock : locks.values()) {
            lockWorker.submit(lock.releaser()).get();
        }
        client.close();
    }

    @VisibleForTesting
    Lock getLockUnsafe(int shard) {
        return locks.get(shard);
    }

    //
    // private methods
    //

    private long nowMillis() {
        return ticker.read() / 1000000;
    }

    private int moreThanHalf() {
        return locks.size() / 2 + 1;
    }

    private boolean isCuratorStarted() {
        return client.getState() == CuratorFrameworkState.STARTED;
    }

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
        if (!isCuratorStarted())
            return "not started";
        else if (client.getZookeeperClient().isConnected())
            return "connected";
        else
            return "not connected";
    }

    public synchronized boolean release(int shard) {
        if (locks.containsKey(shard)) {
            try {
                return lockWorker.submit(locks.get(shard).releaser()).get();
            } catch (InterruptedException ex) {
                log.error("Thread is interrupted:"+ ex.getMessage(), ex);
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
                return lockWorker.submit(locks.get(shard).acquirer()).get();
            } catch (InterruptedException ex) {
                log.error("Thread is interrupted: "+ ex.getMessage(), ex);
                return false;
            } catch (ExecutionException ex) {
                log.error("Acquire error: " + ex.getCause().getMessage(), ex.getCause());
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
        /** Locks in UNKNOWN state will be attemped to be acquired. */
        UNKNOWN,
        /** Lock is acquired by the current blueflood instance. No other blueflood server will have this lock. */
        ACQUIRED,
        /** Attempted to acquire, but failed. Lock is already held by another instance. */
        ACQUIRE_FAILED,
        /** Error state. normally means that the connection is lost. */
        ERROR,
        /** The lock was voluntarily released. Will not attempt to acquire this lock during this time. */
        DISINTERESTED
    }

    class Lock {
        private final int shard;
        private LockState state = LockState.UNKNOWN;
        /** mutex. is non-null unless disconnected from zookeeper. */
        private InterProcessMutex mutex = null;
        private long stateChanged = nowMillis();
        private boolean isAcquiring = false;
        private boolean isReleasing = false;

        Lock(int shard) {
            this.shard = shard;
            checkMutex();
        }

        @Override public String toString() {
            return String.format("shard=%d state=%s isAcquiring=%s isReleasing=%s", shard, state, isAcquiring, isReleasing);
        }

        synchronized void checkMutex() {
            if (mutex == null) {
                mutex = new InterProcessMutex(client, getLockId(shard));
            }
        }

        int getShard() { return shard; }
        boolean isHeld() { return mutex != null && state == LockState.ACQUIRED; }
        boolean isUnheld() { return mutex != null && state == LockState.ACQUIRE_FAILED; }

        boolean isHeldZk() {
            return mutex != null && mutex.isAcquiredInThisProcess();
        }

        /**
         * Performs the maintenance of the lock.
         *
         * <ul>
         *     <li>Move the lock state to {@link LockState#UNKNOWN} if necessary.</li>
         *     <li>Attempt to hold the locks in UNKNOWN state.</li>
         *     <li>Attempt to release the locks that were held for too long.</li>
         * </ul>
         *
         * @param shouldAttempt <code>true</code> if the lock should be attempted to be acquired.
         *
         * @return true if the lock was newly acquired during this cycle. otherwise false.
         */
        boolean performMaintenance(boolean shouldAttempt) {
            updateLockState();

            long now = nowMillis();
            // determine if we should acquire or release.
            if (state == LockState.UNKNOWN && shouldAttempt) {
                acquire();
                return isHeld();
            } else if (state == LockState.ACQUIRED && now - stateChanged > minLockHoldTime.toMillis()) {
                // Lock was held for too long - maybe release to trigger re-balancing.
                float chance = (float)(now - stateChanged - minLockHoldTime.toMillis()) / (float)(Math.max(1, minLockHoldTime.toMillis()));
                float r = rand.nextFloat();
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Will release %s if, %f < %f", shard, r, chance));
                }
                if (r < chance) {
                    release();
                }
            } else if (state == LockState.ERROR && now - stateChanged > minLockHoldTime.toMillis()) {
                log.error("Lock state hasn't toggled from ERROR for " + minLockHoldTime.toString()
                        + "; Client connection status: " + connected);
            }
            return false;
        }

        /**
         * Sees if we should change our lock status to "UNKNOWN". Only locks in UNKNOWN state will periodically
         * be attempted to be acquired.
         */
        synchronized void updateLockState() {
            boolean toUnk = false;
            long now = nowMillis();

            if (state == LockState.DISINTERESTED && now - stateChanged > lockDisinterestedTime.toMillis()) {
                // Lock was voluntarily released long time ago. Attempt to acquire the lock again.
                toUnk = true;
            } else if (state == LockState.ERROR && connected) {
                // Reconnected to Zookeeper.
                toUnk = true;
            } else if (state == LockState.ACQUIRE_FAILED && now - stateChanged > lockDisinterestedTime.toMillis()) {
                // acquisition attempt has failed before long time ago. Attempt to acquire the lock again.
                toUnk = true;
            }

            if (toUnk) {
                setState(LockState.UNKNOWN);
            }
        }

        synchronized LockState getLockState() {
            return state;
        }

        /**
         * Note: locks in error state returns true, because multiple workers working on the same shard is ok.
         */
        synchronized boolean canWork() {
            return state == LockState.ACQUIRED || state == LockState.ERROR;
        }

        synchronized void connectionLost() {
            setState(LockState.ERROR);
            mutex = null;
        }

        synchronized void setState(LockState newState) {
            state = newState;
            stateChanged = nowMillis();
        }

        synchronized void acquire() {
            if (isAcquiring || isReleasing) return;
            isAcquiring = true;
            try {
                log.debug("Acquiring lock for " + shard);
                lockWorker.execute(new FutureTask<Boolean>(acquirer()));
            } catch (RejectedExecutionException ex) {
                log.warn(String.format("Rejected lock execution: active:%d queue:%d shard:%d", lockWorker.getActiveCount(), lockWorker.getQueue().size(), shard));
            }
        }

        synchronized void release() {
            if (isAcquiring || isReleasing) return;
            isReleasing = true;
            try {
                log.debug("Releasing lock for " + shard);
                lockWorker.execute(new FutureTask<Boolean>(releaser()));
            } catch (RejectedExecutionException ex) {
                log.warn(String.format("Rejected lock execution: active:%d queue:%d shard:%d", lockWorker.getActiveCount(), lockWorker.getQueue().size(), shard));
            }
        }

        /**
         * Attempt to acquire a lock.
         * @return <code>true</code> if the lock is in acquired state (already acquired lock will return true).
         */
        synchronized Callable<Boolean> acquirer() {
            return new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    com.codahale.metrics.Timer.Context ctx = lockAcquisitionTimer.time();
                    try {
                        checkMutex();
                        if (!connected || mutex == null) {
                            setState(LockState.ERROR);
                            return false;
                        } else if (state == LockState.ACQUIRED) {
                            return true;
                        } else if (state == LockState.ACQUIRE_FAILED) {
                            return false;
                        } else {
                            log.debug("Trying ZK lock for shard={}", shard);
                            if (mutex.isAcquiredInThisProcess()) {
                                if (log.isTraceEnabled()) {
                                    log.trace("Lock already acquired for shard={}", shard);
                                }
                                return true;
                            } else {
                                try {
                                    boolean acquired = mutex.acquire(ZK_LOCK_TIMEOUT.getValue(), ZK_LOCK_TIMEOUT.getUnit());
                                    if (acquired) {
                                        setState(LockState.ACQUIRED);
                                        log.debug("Acquired ZK lock for shard={}", shard);
                                    } else {
                                        setState(LockState.ACQUIRE_FAILED);
                                        lockAcquisitionFailure.mark();
                                        log.debug("Acquire ZK failed for shard={}", shard);
                                    }
                                    return acquired;
                                } catch (RuntimeException ex) {
                                    log.debug("Exception on ZK acquire for shard={}", shard);
                                    log.warn(ex.getMessage(), ex);
                                    lockErrors.mark();
                                    setState(LockState.ERROR);
                                    return false;
                                }
                            }
                        }
                    } finally {
                        isAcquiring = false;
                        ctx.stop();
                    }
                }
            };
        }

        /**
         * Attempt to release the lock.
         * @return <code>true</code> if the lock is released by this logic. (Already released lock will return false).
         */
        synchronized Callable<Boolean> releaser() {
            return new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    try {
                        checkMutex();
                        if (!connected || mutex == null) {
                            setState(LockState.ERROR);
                            return true;
                        } else if (state != LockState.ACQUIRED) {
                            return false;
                        } else if (mutex.isAcquiredInThisProcess()) {
                            log.debug("Releasing lock for shard={}.", shard);
                            mutex.release();
                            setState(LockState.DISINTERESTED);
                            return true;
                        } else {
                            log.error("Held lock not held by this process? shard={}.", shard);
                            setState(LockState.UNKNOWN);
                            return true;
                        }
                    } finally {
                        isReleasing = false;
                    }
                }
            };
        }
    }
}