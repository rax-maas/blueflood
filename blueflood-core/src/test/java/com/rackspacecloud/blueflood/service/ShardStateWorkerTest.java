package com.rackspacecloud.blueflood.service;

import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.utils.Util;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardStateWorkerTest {
    
    // Collection<Integer> allShards, ShardStateManager shardStateManager, TimeValue period
    @Test
    public void testStartStop() throws Exception {
        Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));
        ShardStateManager manager = new ShardStateManager(allShards, new Ticker() {
            @Override
            public long read() {
                return System.currentTimeMillis();
            }
        });
        
        final AtomicInteger counter = new AtomicInteger(0);
        final ShardStateWorker worker = new ShardStateWorker(allShards, manager, new TimeValue(100, TimeUnit.MILLISECONDS), null) {
            @Override
            void performOperation() {
                counter.incrementAndGet();
            }
        };
        
        final AtomicInteger errorCounter = new AtomicInteger(0);
        // we want starting/stopping to happen in other threads to verify that there are no synchronization usage errors.
        Runnable starter = new Runnable() {
            @Override
            public void run() {
                new Thread() {
                    public void run() {
                        try {
                            worker.setActive(true);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            errorCounter.incrementAndGet();
                        }
                    }
                }.start();
                try { Thread.sleep(100); } catch (Exception ex) {};
            }
        };
        Runnable stopper = new Runnable() {
            @Override
            public void run() {
                new Thread() {
                    public void run() {
                        try {
                            worker.setActive(false);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            errorCounter.incrementAndGet();
                        }
                    }
                }.start();
                try { Thread.sleep(100); } catch (Exception ex) {};
            }
        };
        
        int lastCount;
        
        // initially do nothing.
        stopper.run();
        
        Thread workerThread = new Thread(worker, "worker thread");
        workerThread.start();
        
        // verify that it doesn't start out of the gate.
        Thread.sleep(1000);
        Assert.assertEquals(0, counter.get());
        lastCount = counter.get();
        
        starter.run();
        Thread.sleep(1000);
        stopper.run();
        
        // ensure count grew. it should have been hit several times.
        Assert.assertTrue(counter.get() > lastCount);
        lastCount = counter.get();
        
        Thread.sleep(1000);
        
        // counter should not have grown.
        Assert.assertEquals(lastCount, counter.get());
        
        
        starter.run();
        Thread.sleep(1000);
        stopper.run();
        
        // ensure count grew.
        Assert.assertTrue(counter.get() > lastCount);
    }
    
    
}
