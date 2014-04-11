package com.rackspacecloud.blueflood.dw.ingest;

import com.sun.jersey.api.core.ResourceConfig;
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
        
        // create resources.
        final NotDOAHealthCheck notDOA = new NotDOAHealthCheck();
        final BasicIngestResource basicIngestResource = new BasicIngestResource();
        
        // register resources.
        environment.healthChecks().register("not-doa", notDOA);
        environment.jersey().register(basicIngestResource);
        
        // set a filter that does the local durablity (later)
        //environment.jersey().enable(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS);
        //environment.jersey().property(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, LocalDurabilityFilter.class.getName());
        

    }
}
