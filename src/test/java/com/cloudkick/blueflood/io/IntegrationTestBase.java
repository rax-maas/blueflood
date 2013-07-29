package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.service.Configuration;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.utils.TimeValue;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class IntegrationTestBase {

    static {
        try {
            Configuration.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class AstyanaxTester extends AstyanaxIO {
        // This is kind of gross and has serious room for improvement.
        protected void truncate(String cf) {
            int tries = 3;
            while (tries-- > 0) {
                try {
                    getKeyspace().truncateColumnFamily(cf);
                } catch (ConnectionException ex) {
                    System.err.println("Connection problem, yo. remaining tries: " + tries + " " + ex.getMessage());
                    try { Thread.sleep(1000L); } catch (Exception ewww) {}
                }
            }
        }

        protected final void assertNumberOfRows(String cf, long rows) throws Exception {
            ColumnFamily<String, String> columnFamily = ColumnFamily.newColumnFamily(cf, StringSerializer.get(), StringSerializer.get());
            AstyanaxRowCounterFunction<String, String> rowCounter = new AstyanaxRowCounterFunction<String, String>();
            boolean result = new AllRowsReader.Builder<String, String>(getKeyspace(), columnFamily)
                    .withColumnRange(null, null, false, 0)
                    .forEachRow(rowCounter)
                    .build()
                    .call();
            Assert.assertEquals(rows, rowCounter.getCount());
        }

        public ColumnFamily<Locator, Long> getStringCF() {
            return CF_METRICS_STRING;
        }

        public ColumnFamily<Locator, Long> getFullCF() {
            return CF_METRICS_FULL;
        }

        public ColumnFamily<Long, Locator> getLocatorCF() {
            return CF_METRICS_LOCATOR;
        }

        public MutationBatch createMutationBatch() {
            return getKeyspace().prepareMutationBatch();
        }
    }

    private static final char[] STRING_SEEDS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_".toCharArray();
    private static final Random rand = new Random(System.currentTimeMillis());

    protected final void assertNumberOfRows(String cf, int rows) throws Exception {
        new AstyanaxTester().assertNumberOfRows(cf, rows);
    }

    @Before
    public void setUp() throws Exception {
        // really short lived connections for tests!
        final String[] columnFamilies = {"metrics_locator", "metrics_full", "metrics_5m", "metrics_20m", "metrics_60m",
                                         "metrics_240m", "metrics_1440m", "metrics_state", "metrics_string",
                                         "metrics_metadata"};
        AstyanaxTester truncator = new AstyanaxTester();
        for (String cf : columnFamilies)
            truncator.truncate(cf);
    }

    @After
    public void tearDown() throws Exception {
        // meh
    }
    
    protected Metric writeMetric(String name, Object value) throws Exception {
        final List<Metric> metrics = new ArrayList<Metric>();
        final Locator locator = Locator.createLocatorFromPathComponents("acctId", name);
        Metric metric = new Metric(locator, value, System.currentTimeMillis(),
                new TimeValue(1, TimeUnit.DAYS), "unknown");
        metrics.add(metric);
        AstyanaxWriter.getInstance().insertFull(metrics);

        return metric;
    }

    protected List<Metric> makeRandomIntMetrics(int count) {
        final String accountId = "ac" + randString(8);
        List<Metric> metrics = new ArrayList<Metric>();
        final long now = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            final Locator locator = Locator.createLocatorFromPathComponents(accountId, "met", randString(8));
            metrics.add(getRandomIntMetric(locator, now - 10000000));
        }

        return metrics;
    }

    protected static String randString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(STRING_SEEDS[rand.nextInt(STRING_SEEDS.length)]);
        return sb.toString();
    }

    protected int getRandomIntMetricValue() {
        return rand.nextInt();
    }

    protected Metric getRandomIntMetric(final Locator locator, long timestamp) {
        return new Metric(locator, getRandomIntMetricValue(), timestamp, new TimeValue(1, TimeUnit.DAYS), "unknown");
    }
}
