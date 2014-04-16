package com.rackspacecloud.blueflood.dw.ingest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.rackspacecloud.blueflood.dw.CommonConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

public class IngestConfiguration extends CommonConfiguration {
    
    @NotEmpty 
    private String metricsWriterClass = "com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter";
    
    // debug setting where the API endpoint ignore collection times passed in.
    private boolean forceNewCollectionTime = false;
   
    @JsonProperty
    public String getMetricsWriterClass() { return metricsWriterClass; }

    @JsonProperty
    public void setMetricsWriterClass(String metricsWriterClass) { this.metricsWriterClass = metricsWriterClass; }

    @JsonProperty
    public boolean getForceNewCollectionTime() { return forceNewCollectionTime; }

    @JsonProperty
    public void setforceNewCollectionTime(boolean forceNewCollectionTime) { this.forceNewCollectionTime = forceNewCollectionTime; }
}
