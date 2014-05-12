/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.RestartGauge;
import com.rackspacecloud.blueflood.utils.Util;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BluefloodServiceStarter {
    private static final Logger log = LoggerFactory.getLogger(BluefloodServiceStarter.class);

    public static void validateCassandraHosts() {
        String hosts = Configuration.getInstance().getStringProperty(CoreConfig.CASSANDRA_HOSTS);
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

    private static void startShardStateServices(ScheduleContext context) {
        Configuration config = Configuration.getInstance();
        if (config.getBooleanProperty(CoreConfig.INGEST_MODE) || config.getBooleanProperty(CoreConfig.ROLLUP_MODE)) {
            // these threads are responsible for sending/receiving schedule context state to/from the database.
            final Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));

            try {
                final AstyanaxShardStateIO io = new AstyanaxShardStateIO();
                final ShardStatePusher shardStatePusher = new ShardStatePusher(allShards,
                        context.getShardStateManager(),
                        io);
                final ShardStatePuller shardStatePuller = new ShardStatePuller(allShards,
                        context.getShardStateManager(),
                        io);

                final Thread shardPush = new Thread(shardStatePusher, "Shard state writer");
                final Thread shardPull = new Thread(shardStatePuller, "Shard state reader");

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

    private static void startIngestServices(ScheduleContext context) {
        // start up ingestion services.
        Configuration config = Configuration.getInstance();
        if (config.getBooleanProperty(CoreConfig.INGEST_MODE)) {
            List<String> modules = config.getListProperty(CoreConfig.INGESTION_MODULES);
            if (modules.isEmpty()) {
                log.error("Ingestion mode is enabled, however no ingestion modules are enabled!");
                System.exit(1);
            }
            ClassLoader classLoader = IngestionService.class.getClassLoader();
            final List<IngestionService> ingestionServices = new ArrayList<IngestionService>();
            Integer services_started = 0;
            for (String module : modules) {
                log.info("Loading ingestion service module " + module);
                try {
                    ClassLoader loader = IMetricsWriter.class.getClassLoader();
                    Class writerImpl = loader.loadClass(config.getStringProperty(CoreConfig.IMETRICS_WRITER));
                    IMetricsWriter writer = (IMetricsWriter) writerImpl.newInstance();

                    Class serviceClass = classLoader.loadClass(module);
                    IngestionService service = (IngestionService) serviceClass.newInstance();
                    log.info("Starting ingestion service module " + module + " with writer: " + writerImpl.getSimpleName());
                    ingestionServices.add(service);
                    service.startService(context, writer);
                    log.info("Successfully started ingestion service module " + module + " with writer: " + writerImpl.getSimpleName());
                    services_started++;
                } catch (InstantiationException e) {
                    log.error("Unable to create instance of ingestion service class for: " + module, e);
                    System.exit(1);
                } catch (IllegalAccessException e) {
                    log.error("Error starting ingestion service: " + module, e);
                    System.exit(1);
                } catch (ClassNotFoundException e) {
                    log.error("Unable to locate ingestion service module: " + module, e);
                    System.exit(1);
                } catch (RuntimeException e) {
                    log.error("Error starting ingestion service: " + module, e);
                    System.exit(1);
                } catch (Throwable e) {
                    log.error("Error starting ingestion service: " + module, e);
                    System.exit(1);
                }
            }
            log.info("Started " + services_started + " ingestion services");
        } else {
            log.info("HTTP ingestion service not required");
        }
    }

    private static void startQueryServices() {
        // start up query services.
        Configuration config = Configuration.getInstance();
        if (config.getBooleanProperty(CoreConfig.QUERY_MODE)) {
            List<String> modules = config.getListProperty(CoreConfig.QUERY_MODULES);
            if (modules.isEmpty()) {
                log.error("Query mode is enabled, however no query modules are enabled!");
                System.exit(1);
            }
            ClassLoader classLoader = QueryService.class.getClassLoader();
            final List<QueryService> queryServices = new ArrayList<QueryService>();
            Integer services_started = 0;
            for (String module : modules) {
                log.info("Loading query service module " + module);
                try {
                    Class serviceClass = classLoader.loadClass(module);
                    QueryService service = (QueryService) serviceClass.newInstance();
                    queryServices.add(service);
                    log.info("Starting query service module " + module);
                    service.startService();
                    log.info("Successfully started query service module " + module);
                    services_started++;
                } catch (InstantiationException e) {
                    log.error("Unable to create instance of query service class for: " + module, e);
                    System.exit(1);
                } catch (IllegalAccessException e) {
                    log.error("Error starting query service: " + module, e);
                    System.exit(1);
                } catch (ClassNotFoundException e) {
                    log.error("Unable to locate query service module: " + module, e);
                    System.exit(1);
                } catch (RuntimeException e) {
                    log.error("Error starting query service: " + module, e);
                    System.exit(1);
                } catch (Throwable e) {
                    log.error("Error starting query service: " + module, e);
                    System.exit(1);
                }
            }
            log.info("Started " + services_started + " query services");
        } else {
            log.info("Query service not required");
        }
    }

    private static void startRollupService(final ScheduleContext context) {
        Timer serverTimeUpdate = new java.util.Timer("Server Time Syncer", true);

        if (Configuration.getInstance().getBooleanProperty(CoreConfig.ROLLUP_MODE)) {
            // configure the rollup service. this is a daemonish thread that decides when to rollup ranges of data on
            // in the data cluster.
            final RollupService rollupService = new RollupService(context);
            Thread rollupThread = new Thread(rollupService, "BasicRollup conductor");

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

    private static void startEventListenerModules() {
        Configuration config = Configuration.getInstance();
        List<String> modules = config.getListProperty(CoreConfig.EVENT_LISTENER_MODULES);
        if (!modules.isEmpty()) {
            log.info("Starting event listener modules");
            ClassLoader classLoader = EventListenerService.class.getClassLoader();
            for (String module : modules) {
                log.info("Loading event listener module " + module);
                try {
                    Class serviceClass = classLoader.loadClass(module);
                    EventListenerService service = (EventListenerService) serviceClass.newInstance();
                    log.info("Starting event listener module " + module);
                    service.startService();
                    log.info("Successfully started event listener module " + module);
                } catch (InstantiationException e) {
                    log.error("Unable to create instance of event listener class for: " + module, e);
                } catch (IllegalAccessException e) {
                    log.error("Error starting event listener: " + module, e);
                } catch (ClassNotFoundException e) {
                    log.error("Unable to locate event listener module: " + module, e);
                } catch (RuntimeException e) {
                    log.error("Error starting event listener: " + module, e);
                } catch (Throwable e) {
                    log.error("Error starting event listener: " + module, e);
                }
            }
        } else {
            log.info("No event listener modules configured.");
        }
    }

    public static void main(String args[]) {
        // load configuration.
        Configuration config = Configuration.getInstance();

        // if log4j configuration references an actual file, periodically reload it to catch changes.
        String log4jConfig = System.getProperty("log4j.configuration");
        if (log4jConfig != null && log4jConfig.startsWith("file:")) {
            PropertyConfigurator.configureAndWatch(log4jConfig.substring("file:".length()), 5000);
        }

        // check that we have cassandra hosts
        validateCassandraHosts();
        
        // possibly load the metadata cache
        boolean usePersistedCache = Configuration.getInstance().getBooleanProperty(CoreConfig.METADATA_CACHE_PERSISTENCE_ENABLED);
        if (usePersistedCache) {
            String path = Configuration.getInstance().getStringProperty(CoreConfig.METADATA_CACHE_PERSISTENCE_PATH);
            final File cacheLocation = new File(path);
            if (cacheLocation.exists()) {
                try {
                    DataInputStream in = new DataInputStream(new FileInputStream(cacheLocation));
                    MetadataCache.getInstance().load(in);
                    in.close();
                } catch (IOException ex) {
                    log.error(ex.getMessage(), ex);
                }
            } else {
                log.info("Wanted to load metadata cache, but it did not exist: " + path);
            }
            
            Timer cachePersistenceTimer = new Timer("Metadata-Cache-Persistence");
            int savePeriodMins = Configuration.getInstance().getIntegerProperty(CoreConfig.METADATA_CACHE_PERSISTENCE_PERIOD_MINS);
            cachePersistenceTimer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                DataOutputStream out = new DataOutputStream(new FileOutputStream(cacheLocation, false));
                                MetadataCache.getInstance().save(out);
                                out.close();
                            } catch (IOException ex) {
                                log.error(ex.getMessage(), ex);
                            }
                        }
                    }, 
                    TimeUnit.MINUTES.toMillis(savePeriodMins),
                    TimeUnit.MINUTES.toMillis(savePeriodMins));
        }

        // has the side-effect of causing static initialization of Metrics, starting instrumentation reporting.
        new RestartGauge(Metrics.getRegistry(), RollupService.class);

        final Collection<Integer> shards = Collections.unmodifiableCollection(
                Util.parseShards(config.getStringProperty(CoreConfig.SHARDS)));
        final String zkCluster = config.getStringProperty(CoreConfig.ZOOKEEPER_CLUSTER);
        final ScheduleContext rollupContext = "NONE".equals(zkCluster) ?
                new ScheduleContext(System.currentTimeMillis(), shards) :
                new ScheduleContext(System.currentTimeMillis(), shards, zkCluster);

        log.info("Starting blueflood services");
        startShardStateServices(rollupContext);
        startIngestServices(rollupContext);
        startQueryServices();
        startRollupService(rollupContext);
        startEventListenerModules();
        log.info("All blueflood services started");
    }
}
