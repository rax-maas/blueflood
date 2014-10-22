package com.rackspacecloud.blueflood.service.udp;

import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.service.ShardStateServices;
import com.rackspacecloud.blueflood.service.udp.functions.ContextUpdater;
import com.rackspacecloud.blueflood.service.udp.functions.DeserializeAndReleaseFunc;
import com.rackspacecloud.blueflood.service.udp.functions.SimpleMetricWriter;
import com.rackspacecloud.blueflood.utils.Util;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;

/**
 * This is your main class.  It does several things that are crucial for any metric ingestor:
 * 1. It establishes a context. This gets used by ingestion and rollup processes.
 * 2. It writes the metrics to a database.
 * 3. It keeps shard state updated.
 */
public class MainIngestor {

    private static final Logger log = LoggerFactory.getLogger(MainIngestor.class);
    private static final Configuration config = Configuration.getInstance();

    public static void main(String args[]) {

        // verify the configuration is not broken
        checkConfiguration();

        // establish a context. It will get used by the ingestor and shard state services.
        final Collection<Integer> shards = Collections.unmodifiableCollection(Util.parseShards(config.getStringProperty(CoreConfig.SHARDS)));
        final ScheduleContext rollupContext = new ScheduleContext(System.currentTimeMillis(), shards);

        String host = config.getStringProperty("UDP_BIND_HOST");
        int port = config.getIntegerProperty("UDP_BIND_PORT");
        // start ingesting.
        startIngestion(rollupContext, new InetSocketAddress(host, port));

        // save shard state to/from the database.
        new ShardStateServices(rollupContext, new AstyanaxShardStateIO()).start();
    }

    private static void startIngestion(ScheduleContext context, SocketAddress listenAddress) {

        // UdpListenerService is a fairly generic UDP listener. It does special things once you give it a processor.
        UdpListenerService udpListenerService = new UdpListenerService(listenAddress)
                .withProcessor(buildProcessor(context));
        udpListenerService.start();
    }

    // build a SEDA chain that will process datagrams.
    private static AsyncChain<DatagramPacket, ?> buildProcessor(ScheduleContext context) {

        // this will eventually take a UDP packet, deserialize it and put it to the database. Each w
        AsyncChain<DatagramPacket, ?> processor =

                // this stage deserializes the UDP datagrams. since the serialization is at our discretion (and
                // your's too), it just matters that you are able to end up with a collection of
                // com.rackspacecloud.blueflood.types.Metric.
                AsyncChain.withFunction(new DeserializeAndReleaseFunc(new ThreadPoolBuilder().withName("Packet Deserializer").build()))

                // this this stage writes a single metrics to the database.
                .withFunction(new SimpleMetricWriter(new ThreadPoolBuilder().withName("Database Writer").build()))

                // this stage updates the context, which eventually gets push to the database.
                .withFunction(new ContextUpdater(new ThreadPoolBuilder().withName("Context Updater").build(), context))
                .build();

        return processor;
    }

    // initialize the configuration and validate a few things.
    private static void checkConfiguration() {
        if (!config.getBooleanProperty(CoreConfig.INGEST_MODE)) {
            log.error("Ingestion mode not enabled. Please check your configuration");
            System.exit(StopReasons.INCORRECT_CONFIGURATION);
        }

        log.info("Configuration is good");

        boolean useZookeeper = !"NONE".equals(config.getStringProperty(CoreConfig.ZOOKEEPER_CLUSTER));
        log.info("Using zookeeper? " + useZookeeper);
    }
}
