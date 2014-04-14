package com.rackspacecloud.blueflood.dw.ingest;

import com.google.common.base.Charsets;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import io.dropwizard.cli.ServerCommand;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class IngestionService implements com.rackspacecloud.blueflood.service.IngestionService {
    private static final String YAML = 
            "host: %s\n"+
            "port: %d\n"+
            "cassandraHosts:[%s],\n"+
            "rollupKeyspace: %s\n"+
            "metricsWriterClass: \"%s\"\n"+
            "scopingTenants:[%s]\n"+
            "forceNewCollectionTime: %s";

    public IngestionService() {
    }

    @Override
    public void startService(ScheduleContext context, IMetricsWriter writer) {
     
        final Configuration config = Configuration.getInstance();
        
        final IngestApplication ingestApplication = new IngestApplication();
        final Bootstrap<IngestConfiguration> bootstrap = new Bootstrap<IngestConfiguration>(ingestApplication);
        bootstrap.setConfigurationSourceProvider(new ConfigurationSourceProvider() {
            @Override
            public InputStream open(String path) throws IOException {
                
                
                String replaced = String.format(YAML,
                        // gotta use strings, because I don't want to depend on the http module.
                        config.getStringProperty("HTTP_INGESTION_HOST"),
                        config.getStringProperty("HTTP_INGESTION_PORT"),
                        config.getStringProperty(CoreConfig.CASSANDRA_HOSTS),
                        config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE),
                        config.getStringProperty(CoreConfig.IMETRICS_WRITER),
                        "THERE_ARE_NONE",
                        "false");
                return new ByteArrayInputStream(replaced.getBytes(Charsets.UTF_8));
            }
        });
        
        
        ingestApplication.initialize(bootstrap);
        
        final Map<String, Object> namespaceAttrs = new HashMap<String, Object>();
        //namespaceAttrs.put("file", null);
        namespaceAttrs.put("command", "server");
        namespaceAttrs.put("version", null);
        Namespace namespace = new Namespace(namespaceAttrs);
        
        ServerCommand<IngestConfiguration> serverCommand = new ServerCommand<IngestConfiguration>(ingestApplication);
        
        try {
            serverCommand.run(bootstrap, namespace);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
           
    }
}
