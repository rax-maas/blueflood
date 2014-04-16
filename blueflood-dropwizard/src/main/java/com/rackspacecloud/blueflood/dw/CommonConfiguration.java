package com.rackspacecloud.blueflood.dw;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Collections;
import java.util.List;

public class CommonConfiguration extends Configuration {
    @NotEmpty
    private List<String> cassandraHosts = Lists.newArrayList("127.0.0.1:19180");
    
    private int cassandraRequestTimeout = 10000;
    private int cassandraMaxRetries = 5;
    private int cassandraDefaultPort = 19180;
    
    @NotEmpty
    private String rollupKeyspace = "DATA";
    
    private int shardPushPeriod = 2000;
    private int shardPullPeriod = 20000;
    
    @JsonProperty
    public void setCassandraHosts(List<String> l) { this.cassandraHosts = l; }
        
    @JsonProperty
    public List<String> getCassandraHosts() { return Collections.unmodifiableList(cassandraHosts); }
    
    @JsonProperty
    public int getCassandraRequestTimeout() { return cassandraRequestTimeout; }

    @JsonProperty
    public void setCassandraRequestTimeout(int cassandraRequestTimeout) { this.cassandraRequestTimeout = cassandraRequestTimeout; }

    @JsonProperty
    public int getCassandraMaxRetries() { return cassandraMaxRetries; }

    @JsonProperty
    public void setCassandraMaxRetries(int cassandraMaxRetries) { this.cassandraMaxRetries = cassandraMaxRetries; }

    @JsonProperty
    public int getCassandraDefaultPort() { return cassandraDefaultPort; }

    @JsonProperty
    public void setCassandraDefaultPort(int cassandraDefaultPort) { this.cassandraDefaultPort = cassandraDefaultPort; }

    @JsonProperty
    public String getRollupKeyspace() { return rollupKeyspace; }

    @JsonProperty
    public void setRollupKeyspace(String rollupKeyspace) { this.rollupKeyspace = rollupKeyspace; }

    @JsonProperty
    public int getShardPushPeriod() { return shardPushPeriod; }

    @JsonProperty
    public void setShardPushPeriod(int shardPushPeriod) { this.shardPushPeriod = shardPushPeriod; }

    @JsonProperty
    public int getShardPullPeriod() { return shardPullPeriod; }

    @JsonProperty
    public void setShardPullPeriod(int shardPullPeriod) { this.shardPullPeriod = shardPullPeriod; }
}
