package com.rackspacecloud.blueflood.dw.ingest;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.utils.Metrics;
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
            "server:\n"+
            "  applicationConnectors:\n"+
            "    - type: http\n"+
            "      bindHost: %s\n"+
            "      port: %s\n"+
            "  adminConnectors:\n"+
            "    - type: http\n"+
            "      bindHost: %s\n"+
            "      port: %s\n"+
            "cassandraHosts: [%s]\n"+
            "rollupKeyspace: %s\n"+
            "metricsWriterClass: \"%s\"\n"+
            "forceNewCollectionTime: %s";

    public IngestionService() {
    }

    @Override
    public void startService(ScheduleContext context, IMetricsWriter writer) {
     
        final Configuration config = Configuration.getInstance();
        
        final IngestApplication ingestApplication = new IngestApplication();
        final Bootstrap<IngestConfiguration> bootstrap = new Bootstrap<IngestConfiguration>(ingestApplication) {
            
            // we want to bring our own MetricRegistry. The only downside to this is that we miss a few JVM metrics
            // that get registered by DW. See https://github.com/dropwizard/dropwizard/pull/548 for more info.
            @Override
            public MetricRegistry getMetricRegistry() {
                return Metrics.getRegistry();
            }
        };
        
        bootstrap.setConfigurationSourceProvider(new ConfigurationSourceProvider() {
            @Override
            public InputStream open(String path) throws IOException {
                
                
                String replaced = String.format(YAML,
                        // gotta use strings, because I don't want to depend on the http module.
                        config.getStringProperty("HTTP_INGESTION_HOST"),
                        config.getStringProperty("HTTP_INGESTION_PORT"),
                        config.getStringProperty("HTTP_INGESTION_HOST"),
                        Integer.toString(Integer.parseInt(config.getStringProperty("HTTP_INGESTION_PORT"))+1),
                        makeSafeYamlList(config.getStringProperty(CoreConfig.CASSANDRA_HOSTS), ","),
                        config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE),
                        config.getStringProperty(CoreConfig.IMETRICS_WRITER),
                        "false");
                return new ByteArrayInputStream(replaced.getBytes(Charsets.UTF_8));
            }
        });
        
        ingestApplication.initialize(bootstrap);
        
        final Map<String, Object> namespaceAttrs = new HashMap<String, Object>();
        //namespaceAttrs.put("file", null);
        namespaceAttrs.put("command", "server");
        namespaceAttrs.put("version", null);
        namespaceAttrs.put("file", "/this/path/does/not/matter");
        Namespace namespace = new Namespace(namespaceAttrs);
        
        ServerCommand<IngestConfiguration> serverCommand = new ServerCommand<IngestConfiguration>(ingestApplication);
        
        try {
            serverCommand.run(bootstrap, namespace);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }     
    }
    
    // todo: move to common place.
    public static String makeSafeYamlList(String s, String delimiter) {
        String[] parts = s.split(delimiter, -1);
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (sb.length() == 0) {
                sb = sb.append(String.format("\"%s\"", part));
            } else {
                sb = sb.append(String.format(",\"%s\"", part));
            }
        }
        return sb.toString();
    }
}
