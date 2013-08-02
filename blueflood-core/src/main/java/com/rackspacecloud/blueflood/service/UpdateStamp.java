package com.rackspacecloud.blueflood.service;

/**
 * This class basically serves to annotate a timestamp to indicate if it marks an update or a remove action.
 */
public class UpdateStamp {
    private long timestamp;
    private State state;
    private boolean dirty;
    
    public UpdateStamp(long timestamp, State state, boolean dirty) {
        setTimestamp(timestamp);
        setState(state);
        setDirty(dirty);
    }
    
    public void setDirty(boolean b) { dirty = b; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setState(State state) { this.state = state; }
    
    public boolean isDirty() { return dirty; }
    public long getTimestamp() { return timestamp; }
    public State getState() { return state; }
    
    public int hashCode() {
        return (timestamp + state.code).hashCode();
    }
    
    public boolean equals(Object o) {
        if (!(o instanceof UpdateStamp)) return false;
        UpdateStamp other = (UpdateStamp)o;
        return other.timestamp == timestamp && other.state == state;
    }
    
    public String toString() { return timestamp + "," + state.code; }
    
    public enum State {
        // in the database, there are only two states we care about: Active and Rolled.  Running is a ephemeral state
        // during runtime.  It degrades to Active during a save restore (indicating it is not finished and should,
        // therefore, not be considered rolled.
        Active("A"), Running("A"), Rolled("X");
        private final String code;
        private State(String code) {
            this.code = code;
        }
        public String code() { return code; }
    }
}
