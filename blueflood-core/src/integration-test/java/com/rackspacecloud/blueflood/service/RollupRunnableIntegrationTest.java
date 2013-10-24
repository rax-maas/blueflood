package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class RollupRunnableIntegrationTest extends IntegrationTestBase {
    
    // gentle reader: remember, all column families are truncated between tests.
    
    private AstyanaxWriter writer = AstyanaxWriter.getInstance();
    private AstyanaxReader reader = AstyanaxReader.getInstance();
    
    private final Locator normalLocator = Locator.createLocatorFromPathComponents("runnabletest", "just_some_data");
    private final Range range = new Range(0, 5 * 60 * 1000);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        final TimeValue ttl = new TimeValue(24, TimeUnit.HOURS);
        
        ArrayList<Metric> normalMetrics = new ArrayList<Metric>(5);
        
        for (int i = 0; i < 5; i++) {
            long time = i * 30000;
            Metric metric = new Metric(normalLocator, i, time, ttl, "horses");
            normalMetrics.add(metric);
        }
        
        writer.insertFull(normalMetrics);
    }
    
    @Test
    public void testNormalMetrics() throws IOException {
        // full res has 5 samples.
        Assert.assertEquals(5, reader.getSimpleDataToRoll(normalLocator, range).getPoints().size());
        
        // assert nothing in 5m for this locator.
        Assert.assertEquals(0, reader.getBasicRollupDataToRoll(normalLocator, range, AstyanaxIO.CF_METRICS_5M).getPoints().size());
        
        RollupExecutionContext rec = new RollupExecutionContext(Thread.currentThread());
        RollupContext rc = new RollupContext(normalLocator, range, Granularity.FULL);
        RollupRunnable rr = new RollupRunnable(rec, rc);
        rr.run();
        
        // assert something in 5m for this locator.
        Assert.assertEquals(1, reader.getBasicRollupDataToRoll(normalLocator, range, AstyanaxIO.CF_METRICS_5M).getPoints().size());
    }
}
