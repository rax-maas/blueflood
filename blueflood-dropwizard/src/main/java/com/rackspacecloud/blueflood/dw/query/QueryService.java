package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.rackspacecloud.blueflood.dw.ingest.IngestionService;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
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

public class QueryService implements com.rackspacecloud.blueflood.service.QueryService {
    private static final String YAML = "server:\n" +
            "  applicationConnectors:\n" +
            "    - type: http\n" +
            "      bindHost: %s\n" +
            "      port: %s\n" +
            "  adminConnectors:\n" +
            "    - type: http\n" +
            "      bindHost: %s\n" +
            "      port: %s\n" +
            "cassandraHosts: [%s]\n" +
            "rollupKeyspace: %s";
    
    public QueryService() {
    }
    
    @Override
    public void startService() {
        final Configuration config = Configuration.getInstance();
        
        final QueryApplication queryApplication = new QueryApplication();
        final Bootstrap<QueryConfiguration> bootstrap = new Bootstrap<QueryConfiguration>(queryApplication) {
            
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
                        config.getStringProperty("HTTP_QUERY_HOST"),
                        config.getStringProperty("HTTP_QUERY_PORT"),
                        config.getStringProperty("HTTP_QUERY_HOST"),
                        Integer.toString(config.getIntegerProperty("HTTP_QUERY_PORT") + 1),
                        IngestionService.makeSafeYamlList(config.getStringProperty(CoreConfig.CASSANDRA_HOSTS), ","),
                        config.getStringProperty(CoreConfig.ROLLUP_KEYSPACE));
                
                return new ByteArrayInputStream(replaced.getBytes(Charsets.UTF_8));
            }
        });
        
        final Map<String, Object> namespaceAttrs = new HashMap<String, Object>();
        namespaceAttrs.put("command", "server");
        namespaceAttrs.put("version", null);
        namespaceAttrs.put("file", "/this/path/does/not/exist");
        Namespace namespace = new Namespace(namespaceAttrs);
        
        ServerCommand<QueryConfiguration> serverCommand = new ServerCommand<QueryConfiguration>(queryApplication);
        
        try {
            serverCommand.run(bootstrap, namespace);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
