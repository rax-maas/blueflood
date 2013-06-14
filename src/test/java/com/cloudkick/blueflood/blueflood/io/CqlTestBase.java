package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.service.Configuration;
import com.cloudkick.blueflood.types.Locator;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.StringSerializer;
import junit.framework.Assert;
import junit.framework.TestCase;
import telescope.thrift.Metric;
import telescope.thrift.Telescope;
import telescope.thrift.VerificationModel;

import java.io.IOException;
import java.util.*;

public class CqlTestBase extends TestCase {

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

    @Override
    protected void setUp() throws Exception {
        // really short lived connections for tests!
        final String[] columnFamilies = {"metrics_locator", "metrics_full", "metrics_5m", "metrics_20m", "metrics_60m",
                                         "metrics_240m", "metrics_1440m", "metrics_state", "metrics_string",
                                         "metrics_metadata"};
        AstyanaxTester truncator = new AstyanaxTester();
        for (String cf : columnFamilies)
            truncator.truncate(cf);
    }

    @Override
    protected void tearDown() throws Exception {
        // meh
    }
    
    protected Telescope writeMetric(String name, Metric m) throws Exception {
        Map<String, Metric> metrics = new HashMap<String, Metric>();
        metrics.put(name, m);
        
        Telescope tel = new Telescope("id", "checkId", "acctId", "http", "entityId", "target", System.currentTimeMillis(), 1, VerificationModel.ONE);
        tel.setMonitoringZoneId("mzGRD");
        tel.setMetrics(metrics);
        final CloudMonitoringTelescope cloudMonitoringTelescope = new CloudMonitoringTelescope(tel);

        AstyanaxWriter.getInstance().insertFull(cloudMonitoringTelescope.toMetrics());
        return tel;
    }

    protected static Map<String, Metric> makeRandomIntMetrics(String dimension, int count) {
        Map<String, Metric> map = new HashMap<String, Metric>();
        for (int i = 0; i < count; i++) {
            Metric m = new Metric((byte)'i');
            m.setValueI32(rand.nextInt());
            map.put(dimension + "." + randString(8), m);
        }
        return map;
    }

    protected static String randString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(STRING_SEEDS[rand.nextInt(STRING_SEEDS.length)]);
        return sb.toString();
    }

    protected static Map<String, Metric> makeMetrics(String dimension) {
        Map<String, Metric> metrics = new HashMap<String, Metric>();

        Metric intmetric = new Metric((byte)'i');
        intmetric.setValueI32(32 + Math.abs(rand.nextInt(10)));
        if (dimension != null)
            metrics.put(dimension + ".intmetric", intmetric);
        else
            metrics.put("intmetric", intmetric);

        return metrics;
    }

    public static Telescope makeTelescope(String id, String checkId, String acctId, String checkModule, String entityId, String target, long timestamp, String dimension) {
        Telescope tel = new Telescope(id, checkId, acctId, checkModule, entityId, target, timestamp, 1, VerificationModel.ONE);
        tel.setMetrics(makeMetrics(dimension));
        return tel;
    }

}
