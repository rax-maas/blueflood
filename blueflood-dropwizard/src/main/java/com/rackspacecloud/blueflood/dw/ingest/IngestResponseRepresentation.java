package com.rackspacecloud.blueflood.dw.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestResponseRepresentation {
    
    private final String message;
    
    @JsonCreator
    public IngestResponseRepresentation(@JsonProperty("message") String message) {
        this.message = message;
    }
    
    @JsonProperty
    public String getMessage() { return message; }
}
