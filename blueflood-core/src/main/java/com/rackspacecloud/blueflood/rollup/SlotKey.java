/**
 *    Copyright 2014 Square, Inc.
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

package com.rackspacecloud.blueflood.rollup;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable data structure representing the current slot being checked. 3-tuple of (shard, slot, granularity).
 *
 * @author Jeeyoung Kim
 */
public final class SlotKey {
    private final int shard;
    private final int slot;
    private final Granularity granularity;

    private SlotKey(Granularity granularity, int slot, int shard) {
        Preconditions.checkNotNull(granularity);
        Preconditions.checkArgument(shard >= 0, "shard");
        Preconditions.checkArgument(shard < Constants.NUMBER_OF_SHARDS, "shard");
        Preconditions.checkArgument(slot >= 0, "slot");
        Preconditions.checkArgument(slot < granularity.numSlots(), "slot");

        this.shard = shard;
        this.slot = slot;
        this.granularity = granularity;
    }

    public int getShard() {
        return shard;
    }

    public int getSlot() {
        return slot;
    }

    public Granularity getGranularity() {
        return granularity;
    }

    public static SlotKey of(Granularity granularity, int slot, int shard) {
        return new SlotKey(granularity, slot, shard);
    }

    /**
     * Given the encoded slot key, returns the java object of it.
     * For the valid slot keys, this function is inverse of {@link #toString()}.
     * @return decoded {@link SlotKey}, <code>null</code> if it's an invalid slotkey.
     *
     * This function is mostly used in the tests.
     */
    public static SlotKey parse(String string) {
        String[] tokens = string.split(",");
        if (tokens.length != 3) {
            return null;
        }
        Granularity granularity = Granularity.fromString(tokens[0]);
        if (granularity == null) {
            return null;
        }
        try {
            int slot = Integer.parseInt(tokens[1]);
            int shard = Integer.parseInt(tokens[2]);
            return of(granularity, slot, shard);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public Collection<SlotKey> getChildrenKeys() {
        if (granularity == Granularity.FULL) {
            return ImmutableList.of();
        }

        List<SlotKey> result = new ArrayList<SlotKey>();
        Granularity finer;
        try {
            finer = granularity.finer();
        } catch (GranularityException e) {
            throw new AssertionError("Should not occur.");
        }

        int factor = finer.numSlots() / granularity.numSlots();
        for (int i = 0; i < factor; i++) {
            int childSlot = slot * factor + i;
            SlotKey child = SlotKey.of(finer, childSlot, shard);
            result.add(child);
            result.addAll(child.getChildrenKeys());
        }
        return result;
    }

    /**
     * Returns the string representation used to store in the database.
     */
    @Override public String toString() {
        return String.format("%s,%d,%d", granularity.name(), slot, shard).intern();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SlotKey slotKey = (SlotKey) o;

        if (shard != slotKey.shard) return false;
        if (slot != slotKey.slot) return false;
        if (!granularity.equals(slotKey.granularity)) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = shard;
        result = 31 * result + slot;
        result = 31 * result + granularity.hashCode();
        return result;
    }
}
