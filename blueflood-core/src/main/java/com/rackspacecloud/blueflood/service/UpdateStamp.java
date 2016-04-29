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

/**
 * This class basically serves to annotate a timestamp to indicate if it marks
 * an update or a remove action.
 */
public class UpdateStamp {

    private long timestamp;

    /**
     * Whether or not the given slot needs to be re-rolled, or is currently in
     * the process of re-rolling.
     */
    private State state;

    /**
     * Whether or not the info about a slot needs to be persisted to the
     * database.
     */
    private boolean dirty;

    private long lastRollupTimestamp;

    private long lastIngestTimestamp;

    public UpdateStamp(long timestamp, State state, boolean dirty) {
        setTimestamp(timestamp);
        setState(state);
        setDirty(dirty);
    }

    public UpdateStamp(long timestamp, State state, boolean dirty, long lastRollupTimestamp, long lastIngestTimestamp) {
        this(timestamp, state, dirty);
        this.lastRollupTimestamp = lastRollupTimestamp;
        this.lastIngestTimestamp = lastIngestTimestamp;
    }

    public void setDirty(boolean b) { dirty = b; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setState(State state) { this.state = state; }
    public void setLastRollupTimestamp(long lastRollupTimestamp) { this.lastRollupTimestamp = lastRollupTimestamp; }
    public void setLastIngestTimestamp(long lastIngestTimestamp) { this.lastIngestTimestamp = lastIngestTimestamp; }

    public boolean isDirty() { return dirty; }
    public long getTimestamp() { return timestamp; }
    public State getState() { return state; }
    public long getLastRollupTimestamp() { return lastRollupTimestamp; }
    public long getLastIngestTimestamp() { return lastIngestTimestamp; }

    public int hashCode() {
        return (timestamp + state.code).hashCode();
    }
    
    public boolean equals(Object o) {
        if (!(o instanceof UpdateStamp)) return false;
        UpdateStamp other = (UpdateStamp)o;
        return other.timestamp == timestamp && other.state == state;
    }
    
    public String toString() { return timestamp + "," + state.code; }

    /**
     * The current state of a slot.
     *
     * In the database, there are only two states we care about: {@code Active}
     * and {@code Rolled}. {@code Running} is a ephemeral state during runtime.
     * It degrades to {@code Active} during a save restore (indicating it is
     * not finished and should, therefore, not be considered rolled.
     */
    public enum State {

        Active("A"),
        Running("A"),
        Rolled("X");

        private final String code;

        private State(String code) {
            this.code = code;
        }

        public String code() { return code; }
    }
}
