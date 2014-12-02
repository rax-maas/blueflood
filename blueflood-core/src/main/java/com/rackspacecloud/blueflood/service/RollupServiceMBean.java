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

import java.util.Collection;

public interface RollupServiceMBean {
    void setKeepingServerTime(boolean b);
    boolean getKeepingServerTime();
    void setServerTime(long seconds);
    long getServerTime();
    void setPollerPeriod(long l);
    long getPollerPeriod();
    public int getScheduledSlotCheckCount();
    public void forcePoll();
    
    public int getSecondsSinceLastSlotCheck();
    public int getQueuedRollupCount();
    public int getSlotCheckConcurrency();
    public void setSlotCheckConcurrency(int i);
    
    public int getInFlightRollupCount();
    public int getRollupConcurrency();
    public void setRollupConcurrency(int i);
    
    public boolean getActive();
    public void setActive(boolean b);

    /* shard management methods  */
    public void addShard(final Integer shard);
    public void removeShard(final Integer shard);
    public Collection<Integer> getManagedShards();
    
    public Collection<Integer> getRecentlyScheduledShards();

    public Collection<String> getOldestUnrolledSlotPerGranularity(int shard);
}
