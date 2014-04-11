package com.rackspacecloud.blueflood.dw.ingest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Collections;
import java.util.List;

public class IngestConfiguration extends Configuration {
    
    private int port = 19000;
    
    @NotEmpty
    private String host = "localhost";
    
    @NotEmpty
    private List<String> cassandraHosts = Lists.newArrayList("127.0.0.1:19180");
    
    @NotEmpty 
    private String metricsWriterClass = "com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter";
    
    @JsonProperty
    public String getHost() { return host; }
    
    @JsonProperty
    public void setHost(String s) { this.host = s; }
    
    @JsonProperty
    public int getPort() { return port; }
    
    @JsonProperty
    public void setPort(int i) { this.port = i; }
    
    @JsonProperty
    public void setCassandraHosts(List<String> l) { this.cassandraHosts = l; }
    
    @JsonProperty
    public List<String> getCassandraHosts() { return Collections.unmodifiableList(cassandraHosts); }

    @JsonProperty
    public String getMetricsWriterClass() { return metricsWriterClass; }

    @JsonProperty
    public void setMetricsWriterClass(String metricsWriterClass) { this.metricsWriterClass = metricsWriterClass; }
}
