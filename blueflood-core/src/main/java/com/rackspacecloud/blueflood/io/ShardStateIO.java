/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * An interface for reading and writing ShardState
 */
public interface ShardStateIO {

    /**
     * @param shard
     * @return the SlotState objects corresponding to the given shard
     * @throws IOException
     */
    public Collection<SlotState> getShardState(int shard) throws IOException;

    /**
     * Writes slot state for a given granularity and shard
     *
     * @param shard
     * @param slotTimes a map of key=granularity -> value=(map of key=slot -> value=timestamp)
     * @throws IOException
     */
    public void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException;
}
