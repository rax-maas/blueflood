package com.rackspacecloud.blueflood.dw.query.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Paging {
    private final int limit;
    private final int count;
    private final String marker;
    
    @JsonCreator
    public Paging(@JsonProperty("limit") int limit, @JsonProperty("count") int count, @JsonProperty("marker") String marker) {
        this.limit = limit;
        this.count = count;
        this.marker = marker;
    }

    @JsonProperty
    public int getLimit() { return limit; }

    @JsonProperty
    public int getCount() { return count; }

    @JsonProperty
    public String getMarker() { return marker; }
}
