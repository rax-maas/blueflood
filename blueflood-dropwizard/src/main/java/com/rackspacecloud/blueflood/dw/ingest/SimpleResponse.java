package com.rackspacecloud.blueflood.dw.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleResponse {
    
    private final String message;
    
    @JsonCreator
    public SimpleResponse(@JsonProperty("message") String message) {
        this.message = message;
    }
    
    @JsonProperty
    public String getMessage() { return message; }
}
