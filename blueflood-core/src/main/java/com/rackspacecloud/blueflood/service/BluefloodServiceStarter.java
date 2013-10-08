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

import com.rackspacecloud.blueflood.utils.RestartGauge;
import com.rackspacecloud.blueflood.utils.Util;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.GraphiteReporter;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

public class BluefloodServiceStarter {
    private static final Logger log = LoggerFactory.getLogger(BluefloodServiceStarter.class);

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

    private static void startShardStateServices(ScheduleContext context) {
        if (Configuration.getBooleanProperty("INGEST_MODE") || Configuration.getBooleanProperty("ROLLUP_MODE")) {
            // these threads are responsible for sending/receiving schedule context state to/from the database.
            final Collection<Integer> allShards = Collections.unmodifiableCollection(Util.parseShards("ALL"));

            try {
                final ShardStatePusher shardStatePusher = new ShardStatePusher(allShards,
                        context.getShardStateManager());
                final ShardStatePuller shardStatePuller = new ShardStatePuller(allShards,
                        context.getShardStateManager());

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

    private static void startIngestService(ScheduleContext context) {
        // start up ingestion services.
        if (Configuration.getBooleanProperty("INGEST_MODE")) {
            List<String> modules = Configuration.getListProperty("INGESTION_MODULES");
            if (modules.isEmpty()) {
                log.error("Query mode is enabled, however no ingestion modules are enabled!");
            }
            ClassLoader classLoader = IngestionService.class.getClassLoader();
            final List<IngestionService> ingestionServices = new ArrayList<IngestionService>();
            for (String module : modules) {
                try {
                    Class serviceClass = classLoader.loadClass(module);
                    IngestionService service = (IngestionService) serviceClass.newInstance();
                    ingestionServices.add(service);
                    service.startService(context);
                } catch (InstantiationException e) {
                    log.error("Unable to create instance of ingestion service class for: " + module, e);
                    // crash?
                } catch (IllegalAccessException e) {
                    log.error("Error starting ingestion service: " + module, e);
                    // crash?
                } catch (ClassNotFoundException e) {
                    log.error("Unable to locate ingestion service module: " + module, e);
                    // crash?
                }
            }
        } else {
            log.info("HTTP ingestion service not required");
        }
    }

    private static void startQueryService() {
        // start up query services.
        if (Configuration.getBooleanProperty("QUERY_MODE")) {
            List<String> modules = Configuration.getListProperty("QUERY_MODULES");
            if (modules.isEmpty()) {
                log.error("Query mode is enabled, however no query modules are enabled!");
            }
            ClassLoader classLoader = QueryService.class.getClassLoader();
            final List<QueryService> queryServices = new ArrayList<QueryService>();
            for (String module : modules) {
                try {
                    Class serviceClass = classLoader.loadClass(module);
                    QueryService service = (QueryService) serviceClass.newInstance();
                    queryServices.add(service);
                    service.startService();
                } catch (InstantiationException e) {
                    log.error("Unable to create instance of query service class for: " + module, e);
                    // crash?
                } catch (IllegalAccessException e) {
                    log.error("Error starting query service: " + module, e);
                    // crash?
                } catch (ClassNotFoundException e) {
                    log.error("Unable to locate query service module: " + module, e);
                    // crash?
                }
            }
        } else {
            log.info("Query service not required");
        }
    }

    private static void startRollupService(final ScheduleContext context) {
        Timer serverTimeUpdate = new java.util.Timer("Server Time Syncer", true);

        if (Configuration.getBooleanProperty("ROLLUP_MODE")) {
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

    public static void main(String args[]) {
        // load configuration.
        try {
            Configuration.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // if log4j configuration references an actual file, periodically reload it to catch changes.
        String log4jConfig = System.getProperty("log4j.configuration");
        if (log4jConfig != null && log4jConfig.startsWith("file:")) {
            PropertyConfigurator.configureAndWatch(log4jConfig.substring("file:".length()), 5000);
        }

        // check that we have cassandra hosts
        validateCassandraHosts();

        if (!Configuration.getStringProperty("GRAPHITE_HOST").equals("")) {
            // this IF is a hack around the fact that we don't have graphite running in dev or staging environments
            final MetricsRegistry restartRegistry = new MetricsRegistry();
            final Gauge restartGauge = new RestartGauge(restartRegistry, RollupService.class);
            GraphiteReporter.enable(restartRegistry, 60, TimeUnit.SECONDS,
                    Configuration.getStringProperty("GRAPHITE_HOST"),
                    Configuration.getIntegerProperty("GRAPHITE_PORT"),
                    Configuration.getStringProperty("GRAPHITE_PREFIX"));
        }

        final Collection<Integer> shards = Collections.unmodifiableCollection(
                Util.parseShards(Configuration.getStringProperty("SHARDS")));
        final String zkCluster = Configuration.getStringProperty("ZOOKEEPER_CLUSTER");
        final ScheduleContext rollupContext = "NONE".equals(zkCluster) ?
                new ScheduleContext(System.currentTimeMillis(), shards) :
                new ScheduleContext(System.currentTimeMillis(), shards, zkCluster);

        log.info("Starting blueflood services");
        startShardStateServices(rollupContext);
        startIngestService(rollupContext);
        startQueryService();
        startRollupService(rollupContext);
    }
}
