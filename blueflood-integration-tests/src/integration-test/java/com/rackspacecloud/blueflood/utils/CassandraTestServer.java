package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.IntegrationTestConfig;
import com.rackspacecloud.blueflood.io.datastax.DCassandraUtilsIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Manages embedded Cassandra servers for tests. For speculation on running multiple servers with dynamic port binding,
 * see {@link ElasticsearchTestServer}.
 */
public class CassandraTestServer {

    private static final Logger log = LoggerFactory.getLogger(CassandraTestServer.class);
    private final String cassandraVersion =
            Configuration.getInstance().getStringProperty(IntegrationTestConfig.IT_CASSANDRA_CONTAINER_VERSION);

    private GenericContainer cassandra;

    private static final CassandraTestServer INSTANCE = new CassandraTestServer();

    /**
     * Provides a global singleton so that all tests can share a single Cassandra database instead of each test starting
     * its own. Cassandra startup is slow.
     */
    public static CassandraTestServer getInstance() {
        return INSTANCE;
    }

    /**
     * Like {@link #ensureStarted()} but runs the startup in a separate thread so that you can go on to set up other
     * test resources at the same time. Be sure to invoke the Future to await startup before going ahead with tests.
     */
    public Future<Boolean> asyncEnsureStarted() {
        final ExecutorService service = Executors.newSingleThreadExecutor();
        return service.submit(() -> {
            ensureStarted();
            service.shutdown();
            return true;
        });
    }

    /**
     * Starts a Cassandra database that's initialized to work with Blueflood. If such a server is already running, this
     * is a no-op. Current integration tests (as of 16 Sep 2022) rely on Maven to start Cassandra for them, and we don't
     * want to interfere with that, so this will also not start a database if it can detect that another database is
     * already using the configured cqlsh port. Doing so should make this utility flexible enough to start using so that
     * integration tests can run either in Maven, using the provided database, or be started individually from IntelliJ.
     *
     * For the future, plan to migrate all tests to use this class so that we can manage Cassandra through
     * TestContainers.
     */
    public void ensureStarted() {
        int cqlshPort = Configuration.getInstance().getIntegerProperty(CoreConfig.CASSANDRA_BINXPORT_PORT);
        int thriftPort = Configuration.getInstance().getIntegerProperty(CoreConfig.DEFAULT_CASSANDRA_PORT);
        if (isExternalCassandraRunning(cqlshPort)) {
            log.info("Not starting embedded Cassandra. Port " + cqlshPort + " is already open.");
            return;
        }
        if (cassandra == null || !cassandra.isRunning()) {
            String cqlPortBinding = String.format("%d:9042", cqlshPort);
            String thriftPortBinding = String.format("%d:9160", thriftPort);
            cassandra = new GenericContainer(DockerImageName.parse("cassandra:" + cassandraVersion))
                    .withEnv("JVM_EXTRA_OPTS", "-Dcassandra.skip_wait_for_gossip_to_settle=0");
            cassandra.setPortBindings(Arrays.asList(cqlPortBinding, thriftPortBinding));
            cassandra.start();
            log.info("Waiting for Cassandra...");
            // Have to wait for Cassandra to respond. The init will fail a few times while the container is starting.
            await().atMost(30, TimeUnit.SECONDS).until(() -> {
                try {
                    new DCassandraUtilsIO().initDb();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            });
            log.info("Cassandra is up!");
        }
    }

    /**
     * Checks if there's already a database running by checking if the configured database port is accepting connections.
     */
    private boolean isExternalCassandraRunning(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Stops Cassandra. Don't do this regularly during integration tests because it takes a relatively long time to
     * start. Prefer to keep the same instance running for the entire suite, unless you really have to restart it for
     * some reason.
     */
    public void stop() {
        cassandra.stop();
        cassandra = null;
    }
}
