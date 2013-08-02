package com.rackspacecloud.blueflood.stress;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.io.NumericSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.scribe.ConnectionException;
import com.rackspacecloud.blueflood.scribe.LogException;
import com.rackspacecloud.blueflood.scribe.ScribeClient;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.RollupServiceMBean;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.utils.Util;
import com.netflix.astyanax.model.Column;
import org.junit.Assert;
import org.junit.Test;
import scribe.thrift.LogEntry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class IngressBlackbox extends IntegrationTestBase {
    private static final boolean WRITING = true;
    private static final long START_DATE = 1335796192139L;

    @Override
    public void setUp() throws Exception {
        if (WRITING)
            super.setUp();
    }

    @Test
    public void testThatRollupsHappen() throws IOException {
        // Mon Apr 30 07:29:52 PDT 2012
        final long days = 3;
        final long endDate = START_DATE + 1000 * 60 * 60 * 24 * days;

        // these values all get reused in ele/tests/blueflood/test-handler.js.  Don't change them here unless you change
        // them there too.
        final String account = "acDUSBABEK"; // external_id==123456
        final String entity = "enIntTest";
        // for future use, here are some numbers that map to shard 0:  327, 345, 444, 467.
        final String agentCheckName = "327"; // maps to shard 0.
        final String agentMetricName = "dim0.sawtooth10";
        final String externalCheckName = "345";
        final String externalMetricName = "sawtooth10";
        final String mzId = "mzGRD";

        if (WRITING) {
            final AtomicLong time = new AtomicLong(START_DATE);
            final int MAX_METRIC = 10;

            Assert.assertNotNull(System.getProperty("BLUEFLOOD_JMX_LIST"));
            Collection<RollupServiceMBean> mbeans = Blaster.buildMBeans();

            for (RollupServiceMBean bean : mbeans) {
                // increasing slot check concurrency increases throughput since we'll be generating old slots at a very
                // rapid pace.
                bean.setRollupConcurrency(20);
                bean.setSlotCheckConcurrency(20);
                bean.setPollerPeriod(250);
                bean.setKeepingServerTime(false);
                bean.setServerTime(START_DATE);
            }

            final String scribeHost = Configuration.getStringProperty("SCRIBE_HOST");
            final int scribePort = Configuration.getIntegerProperty("SCRIBE_PORT");

            ScribeClient client = buildScribeClient(scribeHost, scribePort);
            Map<String, Number> agentMetrics = new HashMap<String, Number>();
            Map<String, Number> externalMetrics = new HashMap<String, Number>();
            List<LogEntry> entries = new ArrayList<LogEntry>();
            int value = 0;
            int maxBatchSize = 40;
            while (time.get() < endDate) {
                int batchSize = 0;
                while (batchSize < maxBatchSize) {
                    long now = time.get();
                    value = (value + 1) % MAX_METRIC;
                    agentMetrics.put(agentMetricName, value);
                    externalMetrics.put(externalMetricName, value);
                    entries.add(new LogEntry("CATEGARY", Blaster.makeStringTelescope(now, account, entity, agentCheckName, null, agentMetrics)));
                    entries.add(new LogEntry("CATEGARY", Blaster.makeStringTelescope(now, account, entity, externalCheckName, mzId, externalMetrics)));
                    agentMetrics.clear(); // not strictly necessary.
                    externalMetrics.clear();
                    time.set(now + 30000); // add 30s
                    batchSize += 2;
                }
                try {
                    boolean ok = client.log(entries);
                    if (!ok)
                        System.err.println("Could not send scribe logs");
                    else {
                        entries.clear();
                        for (RollupServiceMBean service : mbeans)
                            service.setServerTime(time.get());
                    }
                } catch (ConnectionException veryVeryBad) {
                    throw new RuntimeException(veryVeryBad);
                } catch (LogException ex) {
                    System.err.println("Problem JMXing");
                    client = buildScribeClient(scribeHost, scribePort);
                }
            }

            // kick off any final rollups.
            for (RollupServiceMBean bean : mbeans) {
                bean.setServerTime(endDate + 3600000); // +1 hrs.
                bean.forcePoll();
            }

            // at this point we need to wait until the rollup slaves are done.
            while (true) {
                boolean allDone = true;
                for (RollupServiceMBean bean : mbeans) {
                    if (bean.getQueuedRollupCount() > 0)
                        allDone = false;
                    else if (bean.getScheduledSlotCheckCount() > 0)
                        allDone = false;
                    else if (bean.getInFlightRollupCount() > 0)
                        allDone = false;
                    if (!allDone) {
                        try { Thread.currentThread().sleep(100); } catch (Exception ex) {}
                        break;
                    }
                }
                if (allDone) {
                    client.close();
                    break;
                }
            }
        }

        // let's get those points now.
        Locator[] locators = new Locator[] {
            Locator.createLocatorFromPathComponents(account, entity, agentCheckName, agentMetricName),
            Locator.createLocatorFromPathComponents(account, entity, externalCheckName, Util.generateMetricName(externalMetricName, mzId))
        };
        for (Locator locator : locators) {
            AstyanaxReader reader = AstyanaxReader.getInstance();
            int countFull = 0;
            int count5m = 0;
            int count20m = 0;
            int count60m = 0;
            int count4h = 0;
            int count1d = 0;

            // verify that things were written and rollups happened.
            Granularity g = Granularity.FULL;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                countFull++;
            }

            g = Granularity.MIN_5;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                Rollup rollup = (Rollup)c.getValue(NumericSerializer.get(g));
                count5m += rollup.getCount();
            }

            g = Granularity.MIN_20;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                Rollup rollup = (Rollup)c.getValue(NumericSerializer.get(g));
                count20m += rollup.getCount();
            }

            g = Granularity.MIN_60;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                Rollup rollup = (Rollup)c.getValue(NumericSerializer.get(g));
                count60m += rollup.getCount();
            }

            g = Granularity.MIN_240;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                Rollup rollup = (Rollup)c.getValue(NumericSerializer.get(g));
                count4h += rollup.getCount();
            }

            g = Granularity.MIN_1440;
            for (Column<Long> c : reader.getNumericRollups(locator, g, g.snapMillis(START_DATE), endDate)) {
                Rollup rollup = (Rollup)c.getValue(NumericSerializer.get(g));
                count1d += rollup.getCount();
            }

            long expectedPoints = 2 * 60 * 24 * days; // twice a minute * the number of minutes in $days
            Assert.assertEquals(expectedPoints, countFull);
            Assert.assertEquals(expectedPoints, count5m);
            Assert.assertEquals(expectedPoints, count20m);
            Assert.assertEquals(expectedPoints, count60m);
            Assert.assertEquals(expectedPoints, count4h);
            Assert.assertEquals(expectedPoints, count1d);
        }

        // tell the whiskey process waiter that we are for sure done.
        System.out.println("done creating rollups");
    }

    private static ScribeClient buildScribeClient(String host, int port) {
        return new ScribeClient(host, port);
    }
}
