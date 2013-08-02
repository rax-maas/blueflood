package com.rackspacecloud.blueflood.inputs.handlers;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.inputs.formats.CloudMonitoringTelescope;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Util;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import telescope.thrift.*;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class ScribeHandlerIntegrationTest extends IntegrationTestBase {
    private ScheduleContext context;
    private Collection<Integer> manageShards = new HashSet<Integer>();
    private static TSerializer serializer;
    private ScribeHandlerIface myScribeHandler;
    private int telescopeIDCount = 0;

    static {
        serializer = new TSerializer(new TBinaryProtocol.Factory());
    }

    private TelescopeOrRemove createTelescopeOrRemove(int checkId, Metric metric) {
        Telescope tscope = new Telescope(Integer.toString(telescopeIDCount++),
                Integer.toString(checkId),
                "ac82a6cd",
                "http",
                "en1d",
                "target",
                System.currentTimeMillis(),
                1,
                VerificationModel.QUORUM);
        if (!(null == metric)) {
            tscope.putToMetrics("metricName", metric);
        }

        TelescopeOrRemove tor = new TelescopeOrRemove();
        tor.setTelescope(tscope);
        return tor;
    }

    private LogEntry createLogEntry(TelescopeOrRemove tor) throws TException {
        byte[] arr = serializer.serialize((TBase) tor);
        return new LogEntry("test.scribe", DatatypeConverter.printBase64Binary(arr));
    }

    public void setupTestPrerequisites() throws IOException {
        manageShards.add(1);
        manageShards.add(5);
        manageShards.add(7);
        context = new ScheduleContext(System.currentTimeMillis() - 1000L, manageShards);
        Configuration.init();
        myScribeHandler = new AlternateScribeHandler(context);
    }

    @Test
    public void testLog() throws IOException, TException{
        setupTestPrerequisites();
        Assert.assertNotNull(myScribeHandler);

        List<LogEntry> entries = new ArrayList<LogEntry>();
        for (int i = 0; i < 10; i++) {
            TelescopeOrRemove tor = createTelescopeOrRemove(i+2, null);
            entries.add(createLogEntry(tor));
        }
        ResultCode myResultCode = myScribeHandler.Log(entries);

        Assert.assertEquals(ResultCode.OK, ResultCode.findByValue(myResultCode.getValue()));


    }

    @Test
    public void testUnitPersistence() throws IOException, TException, InterruptedException {
        setupTestPrerequisites();

        MetadataCache cache = MetadataCache.getInstance();
        List<LogEntry> entries;
        int checkId = 50;
        ResultCode myResultCode;
        Metric metric = Util.createMetric(42);
        metric.setUnitEnum(UnitEnum.BYTES);
        TelescopeOrRemove tor = createTelescopeOrRemove(checkId, metric);
        Telescope tscope = tor.getTelescope();
        CloudMonitoringTelescope cloudMonitoringTelescope = new CloudMonitoringTelescope(tscope);
        com.rackspacecloud.blueflood.types.Metric bfMetric = cloudMonitoringTelescope.toMetrics().get(0);

        entries = new ArrayList<LogEntry>();
        entries.add(createLogEntry(tor));
        myResultCode = myScribeHandler.Log(entries);
        Assert.assertEquals(ResultCode.OK, ResultCode.findByValue(myResultCode.getValue()));
        Thread.sleep(20000); // Unit and type processing occurs in a thread. No guarantee of completion before return of Log()

        Locator loc = Locator.createLocatorFromPathComponents(tscope.getAcctId(), tscope.getEntityId(),
                tscope.getCheckId(), Util.generateMetricName("metricName", tscope.getMonitoringZoneId()));
        try {
            Object u = cache.get(loc, "unit");
            Object t = cache.get(loc, "type");
            Assert.assertEquals(UnitEnum.BYTES.toString().toLowerCase(), u);
            Assert.assertEquals(bfMetric.getType().toString(), t);
        } catch (CacheException e) {
            e.printStackTrace();
            Assert.assertNull(e);
        }

        metric = Util.createMetric("testString");
        metric.setUnitEnum(UnitEnum.OTHER);
        metric.setUnitOtherStr("responses");
        tor = createTelescopeOrRemove(checkId, metric);
        tscope = tor.getTelescope();
        cloudMonitoringTelescope = new CloudMonitoringTelescope(tscope);
        bfMetric = cloudMonitoringTelescope.toMetrics().get(0);

        entries = new ArrayList<LogEntry>();
        entries.add(createLogEntry(tor));
        myResultCode = myScribeHandler.Log(entries);
        Assert.assertEquals(ResultCode.OK, ResultCode.findByValue(myResultCode.getValue()));
        Thread.sleep(2000); // Unit and type processing occurs in a thread. No guarantee of completion before return of Log()

        try {
            Object u = cache.get(loc, "unit");
            Object t = cache.get(loc, "type");
            Assert.assertEquals("responses", u);
            Assert.assertEquals(bfMetric.getType().toString(), t);
        } catch (CacheException e) {
            e.printStackTrace();
            Assert.assertNull(e);
        }
    }
}
