package com.cloudkick.blueflood.service;

import com.cloudkick.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.io.CqlTestBase;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.utils.Util;
import com.netflix.astyanax.model.Column;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import telescope.thrift.Telescope;
import telescope.thrift.VerificationModel;

import java.util.ArrayList;
import java.util.Collection;

// this test used to measure rollups that were refused, but we don't care anymore. Not it makes sure that all rollups
// get executed.
public class RollupThreadpoolIntegrationTest extends CqlTestBase {
    private static final Integer threadsInRollupPool = 2;
    
    static {
        // run this test with a configuration so that threadpool queue size is artificially constrained smaller.
        System.setProperty("MAX_ROLLUP_THREADS", threadsInRollupPool.toString());
    }
    
    // remember: this tests behavior, not performance.
    public void testManyLocators() throws Exception {
        assertEquals(Configuration.getIntegerProperty("MAX_ROLLUP_THREADS"), threadsInRollupPool.intValue());
        
        final String checkName = "testManyLocators";
        int shardToTest = 0;

        // I want to see what happens when RollupService.rollupExecutors gets too much work. It should never reject
        // work.  
        long time = 1234;

        // now we need to write data that will generate an enormous amount of locators.
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        Telescope tel = new Telescope("telId", checkName, "acctId", "checkModule", "entityId", "target", time * 1000, 1, VerificationModel.ONE);
        Collection<Telescope> telescopes = new ArrayList<Telescope>();
        telescopes.add(tel);
       
        final int NUM_LOCATORS = 5000;
        int locatorCount = 0;
        while (locatorCount < NUM_LOCATORS) {
            // generate 100 random metrics.
            tel.setMetrics(CqlTestBase.makeRandomIntMetrics("dim0", 100));
            CloudMonitoringTelescope cmTelescope = new CloudMonitoringTelescope(tel);
            writer.insertFull(cmTelescope.toMetrics());
            locatorCount += 100;
        }

        // lets see how many locators this generated. We want it to be a lot.

        int locatorsForTestShard = 0;
        for (Column<Locator> locator : AstyanaxReader.getInstance().getAllLocators(shardToTest)) {
            locatorsForTestShard++;
        }

        // Make sure number of locators for test shard is greater than number of rollup threads.
        // This is required so that rollups would be rejected for some locators.
        assertTrue(threadsInRollupPool < locatorsForTestShard);

        // great. now lets schedule those puppies.
        ScheduleContext ctx = new ScheduleContext(time, Util.parseShards(String.valueOf(shardToTest)));
        RollupService rollupService = new RollupService(ctx);
        rollupService.setKeepingServerTime(false);
        
        // indicate arrival (which we forced using Writer).
        ctx.update(time, shardToTest);

        // move time forward
        time += 500000;
        ctx.setCurrentTimeMillis(time);
        
        // start the rollups.
        Thread rollupThread = new Thread(rollupService, "rollup service test");
        rollupThread.start();
        
        MetricsRegistry registry = Metrics.defaultRegistry();
        Timer rollupsTimer = (Timer)registry.allMetrics().get(new MetricName("com.cloudkick.blueflood.service", "RollupService", "Rollup Execution Timer"));
        
        assertNotNull(rollupsTimer);
        
        // wait up to 120s for those rollups to finish.
        long start = System.currentTimeMillis();
        while (true) {
            try { Thread.currentThread().sleep(1000); } catch (Exception ex) { }
            if (rollupsTimer.count() >= locatorsForTestShard)
                break;
            assertTrue(String.format("rollups:%d", rollupsTimer.count()), System.currentTimeMillis() - start < 120000);
        }
        
        // make sure there were some that were delayed. If not, we need to increase NUM_LOCATORS.
        assertTrue(rollupsTimer.count() > 0);
    }
}
