package com.rackspacecloud.blueflood.dw.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestResponseRepresentation {
    private String message;
    
    public IngestResponseRepresentation() {
        
    }
    
    @JsonProperty
    public String getMessage() { return message; }
    
    @JsonProperty
    public void setMessage(String s) { this.message = s; }
}
