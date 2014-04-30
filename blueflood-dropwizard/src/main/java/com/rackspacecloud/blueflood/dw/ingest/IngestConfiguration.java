package com.rackspacecloud.blueflood.dw.ingest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.dw.CommonConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Collections;
import java.util.List;

public class IngestConfiguration extends CommonConfiguration {

    private String host = "127.0.0.1";
    private int port = 19000;
    
    @NotEmpty
    private List<String> cassandraHosts = Lists.newArrayList("127.0.0.1:19180");
    
    private int cassandraRequestTimeout = 10000;
    private int cassandraMaxRetries = 5;
    private int cassandraDefaultPort = 19180;
    
    @NotEmpty
    private String rollupKeyspace = "DATA";
    
    private int shardPushPeriod = 2000;
    private int shardPullPeriod = 20000;
    
    @NotEmpty 
    private String metricsWriterClass = "com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter";
    
    // debug setting where the API endpoint ignore collection times passed in.
    private boolean forceNewCollectionTime = false;
    
    private Riemann riemann;
    private Graphite graphite;
    
    private boolean populateBluefloodConfigurationSettings = false;

    @JsonProperty
    public String getHost() { return host; }

    @JsonProperty
    public void setHost(String host) { this.host = host; } 

    @JsonProperty
    public int getPort() { return port; }

    @JsonProperty
    public void setPort(int port) { this.port = port; }

    @JsonProperty
    public void setCassandraHosts(List<String> l) { this.cassandraHosts = l; }
    
    @JsonProperty
    public List<String> getCassandraHosts() { return Collections.unmodifiableList(cassandraHosts); }

    @JsonProperty
    public String getMetricsWriterClass() { return metricsWriterClass; }

    @JsonProperty
    public void setMetricsWriterClass(String metricsWriterClass) { this.metricsWriterClass = metricsWriterClass; }

    @JsonProperty
    public boolean getForceNewCollectionTime() { return forceNewCollectionTime; }

    @JsonProperty
    public void setforceNewCollectionTime(boolean forceNewCollectionTime) { this.forceNewCollectionTime = forceNewCollectionTime; }

    @JsonProperty
    public boolean isPopulateBluefloodConfigurationSettings() { return populateBluefloodConfigurationSettings; }

    @JsonProperty
    public void setPopulateBluefloodConfigurationSettings(boolean populateBluefloodConfigurationSettings) {
        this.populateBluefloodConfigurationSettings = populateBluefloodConfigurationSettings; 
    }

    @JsonProperty
    public Riemann getRiemann() { return riemann; }

    @JsonProperty
    public void setRiemann(Riemann riemann) { this.riemann = riemann; }

    @JsonProperty
    public Graphite getGraphite() { return graphite; }

    @JsonProperty
    public void setGraphite(Graphite graphite) { this.graphite = graphite; }
}

// at some point, when DW query is ready, this class will be factored into `BaseConfiguration` which will hold all
// the common configurations to both ingest and query. For now, keep everything here.

class Riemann {
    private boolean enabled = false;
    private String host;
    private int port;
    private String prefix;
    private String localhost;
    private String[] tags;
    private String separator;
    private int ttl;

    @JsonProperty
    public boolean isEnabled() { return enabled; }

    @JsonProperty
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    @JsonProperty
    public String getHost() { return host; }

    @JsonProperty
    public void setHost(String host) { this.host = host; }

    @JsonProperty
    public int getPort() { return port; }

    @JsonProperty
    public void setPort(int port) { this.port = port; }

    @JsonProperty
    public String getPrefix() { return prefix; }

    @JsonProperty
    public void setPrefix(String prefix) { this.prefix = prefix; }

    @JsonProperty
    public String getLocalhost() { return localhost; }

    @JsonProperty
    public void setLocalhost(String localhost) { this.localhost = localhost; }

    @JsonProperty
    public String[] getTags() { return tags; }

    @JsonProperty
    public void setTags(String[] tags) { this.tags = tags; }

    @JsonProperty
    public String getSeparator() { return separator; }

    @JsonProperty
    public void setSeparator(String separator) { this.separator = separator; }

    @JsonProperty
    public int getTtl() { return ttl; }

    @JsonProperty
    public void setTtl(int ttl) { this.ttl = ttl; }
}

class Graphite {
    private boolean enabled;
    private String host;
    private int port;
    private String prefix;

    @JsonProperty
    public boolean isEnabled() { return enabled; }

    @JsonProperty
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    @JsonProperty
    public String getHost() { return host; }

    @JsonProperty
    public void setHost(String host) { this.host = host; }

    @JsonProperty
    public int getPort() { return port; }

    @JsonProperty
    public void setPort(int port) { this.port = port; }

    @JsonProperty
    public String getPrefix() { return prefix; }

    @JsonProperty
    public void setPrefix(String prefix) { this.prefix = prefix; }
}
