package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.utils.Util;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;


public class IngestApplication extends Application<IngestConfiguration> {
    private static final String NAME = "blueflood-ingest";
    
    public static void main(String[] args) throws Exception {
        String[] preLoadClasses = {
                "javax.servlet.Filter",
                "javax.servlet.DispatcherType",
        };
        for (String className : preLoadClasses) {
            Class.forName(className);
        }
        
        
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
    }

    @Override
    public void run(IngestConfiguration ingestConfiguration, Environment environment) throws Exception {
        final ScheduleContext rollupContext = new ScheduleContext(System.currentTimeMillis(), Util.parseShards("NONE"));
        ClassLoader loader = IMetricsWriter.class.getClassLoader();
        Class writerImpl = loader.loadClass(ingestConfiguration.getMetricsWriterClass());
        IMetricsWriter writer = (IMetricsWriter) writerImpl.newInstance();
        
        //todo: start shard state push/pull service.
        //todo: abstraction for shard state read/write
        
        // create resources.
        final NotDOAHealthCheck notDOA = new NotDOAHealthCheck();
        final BasicIngestResource basicIngestResource = new BasicIngestResource(rollupContext, writer);
        
        // register resources.
        environment.healthChecks().register("not-doa", notDOA);
        environment.jersey().register(basicIngestResource);
        
        // set a filter that does the local durablity (later)
        //environment.jersey().enable(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS);
        //environment.jersey().property(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, LocalDurabilityFilter.class.getName());
        

    }
}
