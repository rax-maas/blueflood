package com.rackspacecloud.blueflood.dw.ingest;

import com.google.common.base.Joiner;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.dw.logging.LogAppenderFactory;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.utils.Util;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.IOException;


public class IngestApplication extends Application<IngestConfiguration> {
    private static final String NAME = "blueflood-ingest";
    
    public static void main(String[] args) throws Exception {        
        new IngestApplication().run(args);
    }

    public IngestApplication() {
        super();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void initialize(Bootstrap<IngestConfiguration> ingestConfigurationBootstrap) {
        
        // possibly initialize custom logging appenders.
        for (LogAppenderFactory factoryDesc : LogAppenderFactory.all()) {
            if (System.getProperty(factoryDesc.getVmFlag()) != null) {
                try {
                    ingestConfigurationBootstrap
                            .getObjectMapper()
                            .getSubtypeResolver()
                            .registerSubtypes(Class.forName(factoryDesc.getAppenderClassName()));
                } catch (ClassNotFoundException ex) {
                    System.err.println(String.format("Could not register logging for: %s", factoryDesc.getAppenderClassName()));
                }
            }
        }
    }
    
    // resets the traditional BF configuation object with values from the YAML configuration.
    public static void overrideBluefloodConfiguration(IngestConfiguration ingestConfiguration) throws IOException {
        // a little bit of backwards compatibility. Take values out of the configuration for this webapp and force them
        // into the traditional blueflood configuration.
        System.setProperty(CoreConfig.CASSANDRA_HOSTS.name(), Joiner.on(",").join(ingestConfiguration.getCassandraHosts()));
        System.setProperty(CoreConfig.CASSANDRA_REQUEST_TIMEOUT.name(), Integer.toString(ingestConfiguration.getCassandraRequestTimeout()));
        System.setProperty(CoreConfig.CASSANDRA_MAX_RETRIES.name(), Integer.toString(ingestConfiguration.getCassandraMaxRetries()));
        System.setProperty(CoreConfig.DEFAULT_CASSANDRA_PORT.name(), Integer.toString(ingestConfiguration.getCassandraDefaultPort()));
        System.setProperty(CoreConfig.ROLLUP_KEYSPACE.name(), ingestConfiguration.getRollupKeyspace().toLowerCase());
        System.setProperty(CoreConfig.SHARD_PUSH_PERIOD.name(), Integer.toString(ingestConfiguration.getShardPushPeriod()));
        System.setProperty(CoreConfig.SHARD_PULL_PERIOD.name(), Integer.toString(ingestConfiguration.getShardPullPeriod()));
        System.setProperty(CoreConfig.IMETRICS_WRITER.name(), ingestConfiguration.getMetricsWriterClass());
        System.setProperty(CoreConfig.INGEST_MODE.name(), Boolean.TRUE.toString());
        
        // pass along the riemann config.
        if (ingestConfiguration.getRiemann() != null && ingestConfiguration.getRiemann().isEnabled()) {
            System.setProperty(CoreConfig.RIEMANN_HOST.name(), ingestConfiguration.getRiemann().getHost());
            System.setProperty(CoreConfig.RIEMANN_PORT.name(), Integer.toString(ingestConfiguration.getRiemann().getPort()));
            System.setProperty(CoreConfig.RIEMANN_PREFIX.name(), ingestConfiguration.getRiemann().getPrefix());
            System.setProperty(CoreConfig.RIEMANN_LOCALHOST.name(), ingestConfiguration.getRiemann().getLocalhost());
            System.setProperty(CoreConfig.RIEMANN_TAGS.name(), Joiner.on(",").join(ingestConfiguration.getRiemann().getTags()));
            System.setProperty(CoreConfig.RIEMANN_SEPARATOR.name(), ingestConfiguration.getRiemann().getSeparator());
            System.setProperty(CoreConfig.RIEMANN_TTL.name(), Integer.toString(ingestConfiguration.getRiemann().getTtl()));
        }
        
        // pass along the graphite config.
        if (ingestConfiguration.getGraphite() != null && ingestConfiguration.getGraphite().isEnabled()) {
            System.setProperty(CoreConfig.GRAPHITE_HOST.name(), ingestConfiguration.getGraphite().getHost());
            System.setProperty(CoreConfig.GRAPHITE_PORT.name(), Integer.toString(ingestConfiguration.getGraphite().getPort()));
            System.setProperty(CoreConfig.GRAPHITE_HOST.name(), ingestConfiguration.getGraphite().getPrefix());
        }
        
        Configuration.getInstance().init();
    }

    @Override
    public void run(IngestConfiguration ingestConfiguration, Environment environment) throws Exception {
        
        // should we inject the Dropwizard config into Blueflood? 
        if (ingestConfiguration.isPopulateBluefloodConfigurationSettings()) {
            overrideBluefloodConfiguration(ingestConfiguration);
        }
        
        final ScheduleContext rollupContext = new ScheduleContext(System.currentTimeMillis(), Util.parseShards("NONE"));
        
        // construct the ingestion writer.
        ClassLoader loader = IMetricsWriter.class.getClassLoader();
        Class writerImpl = loader.loadClass(ingestConfiguration.getMetricsWriterClass());
        IMetricsWriter writer = (IMetricsWriter) writerImpl.newInstance();
        
        
        // state management for active shards, slots, etc.
        ShardStateIO shardstateIO = new AstyanaxShardStateIO(); // todo: use configuration setting.
        StateManager stateManager = new StateManager(rollupContext, shardstateIO);
        environment.lifecycle().manage(stateManager);
        
        MetadataCache cache = MetadataCache.getInstance();
        
        // create resources.
        final NotDOAHealthCheck notDOA = new NotDOAHealthCheck();
        final BasicIngestResource basicIngestResource = new BasicIngestResource(
                ingestConfiguration,
                rollupContext,
                writer,
                cache);
        final MultiTenantIngestResource mtIngestResource = new MultiTenantIngestResource(
                ingestConfiguration,
                rollupContext,
                writer,
                cache);
        
        // register resources.
        environment.healthChecks().register("not-doa", notDOA);
        environment.jersey().register(basicIngestResource);
        environment.jersey().register(mtIngestResource);
        
        // set a filter that does the local durablity (later)
        //environment.jersey().enable(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS);
        //environment.jersey().property(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, LocalDurabilityFilter.class.getName());
        

    }
}
