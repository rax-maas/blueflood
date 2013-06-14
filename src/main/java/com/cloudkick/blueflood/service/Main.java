package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.inputs.handlers.AlternateScribeHandler;
import com.cloudkick.blueflood.inputs.handlers.ScribeHandlerIface;
import com.cloudkick.blueflood.thrift.ThriftRunnable;
import com.cloudkick.blueflood.thrift.UnrecoverableException;
import com.cloudkick.blueflood.utils.RestartGauge;
import com.cloudkick.blueflood.utils.StatsEmitter;
import com.cloudkick.blueflood.utils.Util;
import com.cloudkick.blueflood.utils.Version;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.GraphiteReporter;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.scribe;
import telescope.thrift.RollupServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * This is where the Rollup service starts.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void validateCassandraHosts() {
        String hosts = Configuration.getStringProperty("CASSANDRA_HOSTS");
        if (!(hosts.length() >= 3)) {
            log.error("No cassandra hosts found in configuration option 'CASSANDRA_HOSTS'");
            System.exit(-1);
        }
        for (String host : hosts.split(",")) {
            if (!host.matches("[\\d\\w\\.]+:\\d+")) {
                log.error("Invalid Cassandra host found in Configuration option 'CASSANDRA_HOSTS' -- Should be of the form <hostname>:<port>");
                System.exit(-1);
            }
        }
    }
    
    // temporary: Use old configuration settings to force current configuration values for run types.
    private static void adjustConfiguration() {
        final Map<Object, Object> properties = new HashMap<Object, Object>(Configuration.getProperties());
        
        // no scribe host means this is not an ingestor.
        properties.put("INGEST_MODE", "NONE".equals(properties.get("SCRIBE_HOST")) ? "false" : "true");
        
        // no telescope host means it definitely is not query node.  We could have also keyed off SHARD_PULL_PERIOD, 
        // but it is less accurate in testing environments.
        properties.put("QUERY_MODE", "NONE".equals(properties.get("TELESCOPE_HOST")) ? "false" : "true");
        
        // every node is technically a rollup node (the thread always started previoiusly).
        properties.put("ROLLUP_MODE", "NONE".equals(properties.get("SHARDS")) ? "false" : "true");
        
        // reset the configuration.
        Configuration.init(new Properties() {{
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }});
    }
    
    private static void startShardStateServices(ScheduleContext context) {
        if (Configuration.getBooleanProperty("INGEST_MODE") || Configuration.getBooleanProperty("ROLLUP_MODE")) {
            // these threads are responsible for sending/receiving schedule context state to/from the database.
            final Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));
            
            try {
                final Thread shardPush = new Thread(new ShardStatePusher(allShards, context), "Shard state writer");
                final Thread shardPull = new Thread(new ShardStatePuller(allShards, context), "Shard state reader");
                
                shardPull.start();
                shardPush.start();
                
                log.info("Shard push and pull services started");
            } catch (NumberFormatException ex) {
                log.error("Shard services not started. Probably misconfiguration", ex);
            }
        } else {
            log.info("Shard push and pull services not required");
        }
    }
    
    private static void startIngestService(ScheduleContext context, String version) {
        // start up ingestion services.
        if (Configuration.getBooleanProperty("INGESTION_MODE")) {
            ScribeHandlerIface scribeHandler = new AlternateScribeHandler(context);
            scribe.Processor processor = new scribe.Processor(scribeHandler.getScribe());
        

            final BlockingQueue<Runnable> thriftOverflowQueueScribe = new SynchronousQueue<Runnable>();
            final ExecutorService executorServiceScribe = new ThreadPoolExecutor(Configuration.getIntegerProperty("MIN_THREADS"),
                                                                           Configuration.getIntegerProperty("MAX_THREADS"),
                                                                           60, TimeUnit.SECONDS,
                                                                           thriftOverflowQueueScribe);
            ThriftRunnable scribeRunnable = new ThriftRunnable(
                Configuration.getStringProperty("SCRIBE_HOST"),
                Configuration.getIntegerProperty("SCRIBE_PORT"),
                "Blueflood",
                processor,
                Configuration.getIntegerProperty("THRIFT_RPC_TIMEOUT"),
                Configuration.getIntegerProperty("THRIFT_LENGTH"),
                executorServiceScribe);
    
            Thread scribeListeningThread = new Thread(scribeRunnable, "Scribe Threadpools") {
                public void run() {
                    try {
                        super.run();
                    } catch (UnrecoverableException ex) {
                        log.error("Caught unrecoverable exception " + ex.getMessage(), ex);
                    } catch (RuntimeException ex) {
                        log.error("Caught runtime exception " + ex.getMessage(), ex);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    } finally {
                        System.exit(-1);
                    }
                }
            };
            
            scribeListeningThread.start();
            log.info("Listening for scribe clients to send telescopes");
            
        } else {
            log.info("Scribe ingestion service not required");
        }
    }
    
    private static void startQueryService() {
        // start up query services.
        if (Configuration.getBooleanProperty("QUERY_MODE")) {
            String metricsQuery = "Metrics Query";
            RollupServer.Iface rollupHandler = new RollupHandler();
            RollupServer.Processor rollupProcessor = new RollupServer.Processor(rollupHandler);
    
            final BlockingQueue<Runnable> thriftOverflowQueueTelescopes = new ArrayBlockingQueue<Runnable>(
                    Configuration.getIntegerProperty("MAX_THRIFT_OVERFLOW_QUEUE_SIZE"));
            final ExecutorService executorServiceTelescopes = new ThreadPoolExecutor(Configuration.getIntegerProperty("MIN_THREADS"),
                                                                           Configuration.getIntegerProperty("MAX_THREADS"),
                                                                           60, TimeUnit.SECONDS,
                                                                           thriftOverflowQueueTelescopes);
    
            ThriftRunnable rollupRunnable = new ThriftRunnable(
                Configuration.getStringProperty("TELESCOPE_HOST"),
                Configuration.getIntegerProperty("TELESCOPE_PORT"),
                metricsQuery,
                rollupProcessor,
                Configuration.getIntegerProperty("THRIFT_RPC_TIMEOUT"),
                Configuration.getIntegerProperty("THRIFT_LENGTH"),
                executorServiceTelescopes);
    
            Thread telescopeListeningThread = new Thread(rollupRunnable, "Telescope Threadpools") {
                public void run() {
                    try {
                        super.run();
                    } catch (UnrecoverableException ex) {
                        log.error("Caught unrecoverable exception " + ex.getMessage(), ex);
                    } catch (RuntimeException ex) {
                        log.error("Caught runtime exception " + ex.getMessage(), ex);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    } finally {
                        System.exit(-1);
                    }
                }
            };
            
            telescopeListeningThread.start();
            log.info("Listening for telescope clients to service queries");
            
        } else {
            log.info("Telescope query service not required");
        }
    }
    
    private static void startRollupService(final ScheduleContext context) {
        Timer serverTimeUpdate = new java.util.Timer("Server Time Syncer", true);
        
        if (Configuration.getBooleanProperty("ROLLUP_MODE")) {
            // configure the rollup service. this is a daemonish thread that decides when to rollup ranges of data on
            // in the data cluster.
            final RollupService rollupService = new RollupService(context);
            Thread rollupThread = new Thread(rollupService, "Rollup conductor");
            
            // todo: this happens no matter what.
            // todo: at some point, it makes sense to extract the keeping-server-time and setting-server-time methods
            // out of RollupService.  It's a historical artifact at this point.
            
            serverTimeUpdate.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (rollupService.getKeepingServerTime()) {
                        context.setCurrentTimeMillis(System.currentTimeMillis());
                    }
                }
            }, 100, 500);
            
            rollupThread.start();
            
        } else {
            serverTimeUpdate.schedule(new TimerTask() {
                @Override
                public void run() {
                    context.setCurrentTimeMillis(System.currentTimeMillis());
                }
            }, 100, 500);
        }
    } 

    public static void main(String args[]) {
        
        // load configuration.
        try {
            Configuration.init();
            adjustConfiguration();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // if log4j configuration references an actual file, periodically reload it to catch changes.
        String log4jConfig = System.getProperty("log4j.configuration");
        if (log4jConfig != null && log4jConfig.startsWith("file:"))
            PropertyConfigurator.configureAndWatch(log4jConfig.substring("file:".length()), 5000);

        String version = "development";
        try {
            version = Version.getHashVersion();
            if (version == null)
                version = "development";
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // check that we have cassandra hosts
        validateCassandraHosts();

        //statsd
        StatsEmitter stats = StatsEmitter.create(Configuration.getStringProperty("STATSD_HOST"), Configuration.getIntegerProperty("STATSD_PORT"));
        if (!Configuration.getStringProperty("GRAPHITE_HOST").equals("")) {
            // this IF is a hack around the fact that we don't have graphite running in dev or staging environments
            final MetricsRegistry restartRegistry = new MetricsRegistry();
            final Gauge restartGauge = new RestartGauge(restartRegistry, RollupService.class);
            GraphiteReporter.enable(restartRegistry, 60, TimeUnit.SECONDS, Configuration.getStringProperty("GRAPHITE_HOST"), Configuration.getIntegerProperty("GRAPHITE_PORT"), Configuration.getStringProperty("GRAPHITE_PREFIX"));
        }
        
        final Collection<Integer> shards = Collections.unmodifiableCollection(Util.parseShards(Configuration.getStringProperty("SHARDS")));
        final ScheduleContext rollupContext = "NONE".equals(Configuration.getStringProperty("ZOOKEEPER_CLUSTER")) ?
                new ScheduleContext(System.currentTimeMillis(), shards) :
                new ScheduleContext(System.currentTimeMillis(), shards, Configuration.getStringProperty("ZOOKEEPER_CLUSTER"));
        
        log.info("Blueflood starting services");
        
        
        // todo: where does this comment need to go back to?
        // configure scribe server.
        // set the last rollup to be a full period ago. This will force rollups to be [re]created for the current slot.
        
        startShardStateServices(rollupContext);
        // todo: version dependency can be remove by a simple refactor of how AlternateScribeHandler exports the scribe interface.
        startIngestService(rollupContext, version);
        startQueryService();
        startRollupService(rollupContext);
    }
}
