package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.InterruptedOperationException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.cli.*;
import org.apache.commons.lang.RandomStringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class AstyanaxBenchmarker {
    private static final Options cliOptions = new Options();
    private static final String CLUSTER_ID = "TEST_CLUSTER";
    private static final String KEYSPACE = "TEST_KEYSPACE";
    private static final String COLUMNFAMILY = "TEST_CF";
    private static final String CLUSTER = "cluster";
    private static final String MAX_CONNS = "max_conns";
    private static final String POOL_TYPE = "pool_type";
    private static final String DISCOVERY_TYPE = "discovery_type";
    private static final String CONN_TIMEOUT = "conn_timeout";
    private static final String CONCURRENCY = "concurreny";
    private static final String BATCH_SIZE = "batch_size";
    private static final String RUN_TIME = "time";
    private static final String REPLICATION_FACTOR = "replication";
    private static final Integer PORT = 9160;

    private static AstyanaxContext<Keyspace> testContext;
    private static Keyspace keyspaceHandle;
    private static ColumnFamily<String, Long> testCF = new ColumnFamily<String, Long>(COLUMNFAMILY, StringSerializer.get(), LongSerializer.get());

    private static Random randomValueGenerator = new Random();
    private static int batch_size;
    private static long timeToBenchmark;
    private static AtomicBoolean stopBenchmark = new AtomicBoolean(false);

    private static ThreadPoolExecutor benchmarkExecutors;

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("Cassandra cluster").create(CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Max connections per host").create(MAX_CONNS));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Connection Pool type").create(POOL_TYPE));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Node Discovery type").create(DISCOVERY_TYPE));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Cassandra connection timeout").create(CONN_TIMEOUT));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Number of concurrent requests").create(CONCURRENCY));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Batch size").create(BATCH_SIZE));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Time to benchmark").create(RUN_TIME));
        cliOptions.addOption(OptionBuilder.hasArg(true).withDescription("Replication factor to use").create(REPLICATION_FACTOR));
    }

    public static void main(String[] args) {
        Map<String, Object> options = parseOptions(args);
        try {
            setupTest(options);
            runTest();
            printReports();
            cleanUp();
        } catch (Exception e) {
            System.err.println("Exception encountered:" + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        } finally {
            System.out.println("~\t~\t Astyanax Benchmark Completed");
        }
    }

    private static Map<String, Object> parseOptions(String[] args) {
        final GnuParser parser = new GnuParser();
        final Map<String, Object> options = new HashMap<String, Object>();
        try {
            CommandLine line = parser.parse(cliOptions, args);
            options.put(CLUSTER, line.getOptionValue(CLUSTER));
            options.put(MAX_CONNS, line.hasOption(MAX_CONNS) ? Integer.parseInt(line.getOptionValue(MAX_CONNS)) : 50);
            options.put(POOL_TYPE, line.hasOption(POOL_TYPE) ? line.getOptionValue(POOL_TYPE) : "NONE");
            options.put(DISCOVERY_TYPE, line.hasOption(DISCOVERY_TYPE) ? line.getOptionValue(DISCOVERY_TYPE) : "NONE");
            options.put(CONN_TIMEOUT, line.hasOption(CONN_TIMEOUT) ? Integer.parseInt(line.getOptionValue(CONN_TIMEOUT)) : 10000);
            options.put(CONCURRENCY, line.hasOption(CONCURRENCY) ? Integer.parseInt(line.getOptionValue(CONCURRENCY)) : 10);
            options.put(BATCH_SIZE, line.hasOption(BATCH_SIZE) ? Integer.parseInt(line.getOptionValue(BATCH_SIZE)) : 100);
            options.put(RUN_TIME, line.hasOption(RUN_TIME) ? Long.parseLong(line.getOptionValue(RUN_TIME)) : 10000L);
            System.out.println(line.getOptionValue(CLUSTER));
            options.put(REPLICATION_FACTOR, line.hasOption(REPLICATION_FACTOR) ? Integer.parseInt(line.getOptionValue(REPLICATION_FACTOR)) : 3);
        } catch (ParseException ex) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("astyanax-benchmark", cliOptions);
            System.exit(-1);
        }

        return options;
    }

    private static void setupTest(Map<String ,Object> options) throws ConnectionException {
        System.out.println("\t~\t Setting up the benchmark");
        NodeDiscoveryType nodeDiscovery;
        String nodeDiscoveryType = (String)options.get(DISCOVERY_TYPE);
        if (nodeDiscoveryType.equals("RING_DESCRIBE")) {
            nodeDiscovery = NodeDiscoveryType.RING_DESCRIBE;
        } else if (nodeDiscoveryType.equals("TOKEN_AWARE")) {
            nodeDiscovery = NodeDiscoveryType.TOKEN_AWARE;
        } else {
            nodeDiscovery = NodeDiscoveryType.NONE;
        }

        ConnectionPoolType connectionPool;
        String connectionPoolType = (String)options.get(POOL_TYPE);
        if (connectionPoolType.equals("BAG")) {
            connectionPool = ConnectionPoolType.BAG;
        } else if (connectionPoolType.equals("TOKEN_AWARE")) {
            connectionPool = ConnectionPoolType.TOKEN_AWARE;
        } else {
            connectionPool = ConnectionPoolType.ROUND_ROBIN;
        }

        AstyanaxConfigurationImpl astyanaxConfiguration = new AstyanaxConfigurationImpl().setDiscoveryType(nodeDiscovery).setConnectionPoolType(connectionPool);

        ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("TestConnectionPool")
                .setPort(PORT)
                .setMaxConns((Integer) options.get(MAX_CONNS))
                .setSeeds((String) options.get(CLUSTER))
                .setMaxTimeoutWhenExhausted((Integer) options.get(CONN_TIMEOUT));

         System.out.println(options.get(CLUSTER));


         testContext = new AstyanaxContext.Builder()
                .forCluster(CLUSTER_ID)
                .forKeyspace(KEYSPACE)
                .withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl())
                .withAstyanaxConfiguration(astyanaxConfiguration)
                .withConnectionPoolConfiguration(connectionPoolConfiguration)
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        testContext.start();
        keyspaceHandle = testContext.getEntity();
        try {
            keyspaceHandle.describeKeyspace();
        } catch (BadRequestException e) {
            keyspaceHandle.createKeyspace(ImmutableMap.<String, Object>builder()
                    .put("strategy_options", ImmutableMap.<String, Object>builder()
                            .put("replication_factor", "1")
                            .build())
                    .put("strategy_class", "SimpleStrategy")
                    .build()
            );
        }
        try {
        keyspaceHandle.createColumnFamily(testCF, null);
        } catch (BadRequestException e) {
            // Pass.TEST_CF already exists.
        }

        benchmarkExecutors = new ThreadPoolExecutor((Integer)options.get(CONCURRENCY), (Integer)options.get(CONCURRENCY), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
        batch_size = (Integer)options.get(BATCH_SIZE);
        timeToBenchmark = (Long)options.get(RUN_TIME);
    }

    private static void cleanUp() throws ConnectionException {
        benchmarkExecutors.shutdownNow();
        keyspaceHandle.dropKeyspace();
    }

    private static void printReports() {
        AtomicBoolean atomicPrint = new AtomicBoolean(false);
        if (stopBenchmark.get() && !atomicPrint.get()) {
            atomicPrint.compareAndSet(false, true);
            benchmarkExecutors.shutdownNow();

            CountingConnectionPoolMonitor monitor = (CountingConnectionPoolMonitor) testContext.getConnectionPoolMonitor();
            String result = String.format("\t~\t~\t~\t Successes : %d \t Failures : %d \t Timeouts : %d \t Failovers : %d \t PoolExhausted %d", monitor.getOperationSuccessCount(), monitor.getOperationFailureCount()
                    , monitor.getOperationTimeoutCount(), monitor.getFailoverCount(), monitor.getPoolExhaustedTimeoutCount());
            System.out.println(result);
        }
    }

    private static void runTest() {
        final long startTime = System.currentTimeMillis();

        System.out.println(String.format("~\t~\t~\t Starting Astyanax benchmark for %d", timeToBenchmark));

        while (!stopBenchmark.get() && !benchmarkExecutors.isShutdown()) {
            benchmarkExecutors.submit(new Runnable() {
                @Override
                public void run() {

                    if (stopBenchmark.get())
                        printReports();

                    MutationBatch m = keyspaceHandle.prepareMutationBatch();
                    String rowKey = null;
                    Long value;
                    for (int i = 0; i < batch_size; i++) {
                        rowKey = RandomStringUtils.random(80);
                        value = randomValueGenerator.nextLong();
                        m.withRow(testCF, rowKey)
                                .putColumn(System.currentTimeMillis(), value, null);
                    }
                    try {
                        m.execute();
                        System.out.println("Batch inserted for rowKey " + rowKey);
                    } catch (InterruptedOperationException e) {
                        //pass
                    } catch (ConnectionException e) {
                        System.err.println("Error encountered:" + e.getStackTrace());
                        System.exit(-1);
                    } finally {
                        if (System.currentTimeMillis() - startTime > timeToBenchmark) {
                            System.out.println("Exceeded time for benchmark");
                            stopBenchmark.set(true);
                        }
                    }
                }
            });
        }
    }
}
