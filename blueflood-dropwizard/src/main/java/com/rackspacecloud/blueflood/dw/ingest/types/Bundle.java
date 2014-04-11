package com.rackspacecloud.blueflood.dw.ingest.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Bundle implements ICollectionTime {
    private long collectionTime;
    private final List<Gauge> gauges;
    private final List<Counter> counters;
    private final List<Timer> timers;
    private final List<Set> sets;
    
    // optional.
    private long flushInterval = 0;
    
    public Bundle(
            @JsonProperty("collectionTime") long collectionTime,
            @JsonProperty("gauges") List<Gauge> gauges,
            @JsonProperty("counters") List<Counter> counters,
            @JsonProperty("timers") List<Timer> timers,
            @JsonProperty("sets") List<Set> sets) {
        this.collectionTime = collectionTime;
        this.gauges = gauges;
        this.counters = counters;
        this.timers = timers;
        this.sets = sets;
    }

    @JsonProperty
    public long getCollectionTime() {
        return collectionTime;
    }
    
    @JsonProperty
    public void setCollectionTime(long time) {
        collectionTime = time;
    }

    @JsonProperty
    public List<Gauge> getGauges() {
        return safeUnmodifiableList(gauges);
    }

    @JsonProperty
    public List<Counter> getCounters() {
        return safeUnmodifiableList(counters);
    }

    @JsonProperty
    public List<Timer> getTimers() {
        return safeUnmodifiableList(timers);
    }

    @JsonProperty
    public List<Set> getSets() {
        return safeUnmodifiableList(sets);
    }

    @JsonProperty
    public long getFlushInterval() { return flushInterval; }

    @JsonProperty
    public void setFlushInterval(long flushInterval) { this.flushInterval = flushInterval; }

    public static <T> List<T> safeUnmodifiableList(final List<T> list) {
        if (list == null)
            return Collections.unmodifiableList(new java.util.ArrayList<T>());
        else
            return Collections.unmodifiableList(list);
    }
    
    public static <K,V> Map<K,V> safeUnmodifiableMap(Map<? extends K, ? extends V> m) {
        if (m == null)
            return Collections.unmodifiableMap(new HashMap<K, V>());
        else
            return Collections.unmodifiableMap(m);
    }
}
