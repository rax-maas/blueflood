package com.rackspacecloud.blueflood.stress;

import com.rackspacecloud.blueflood.cm.Util;
import com.rackspacecloud.blueflood.scribe.ConnectionException;
import com.rackspacecloud.blueflood.scribe.LogException;
import com.rackspacecloud.blueflood.scribe.ScribeClient;
import com.rackspacecloud.blueflood.service.RollupServiceMBean;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import scribe.thrift.LogEntry;
import telescope.thrift.Metric;
import telescope.thrift.Telescope;
import telescope.thrift.TelescopeOrRemove;
import telescope.thrift.VerificationModel;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Blaster {
    private static final TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory(false, false));
    
    // always try to connect to all endpoints.
    public static Collection<RollupServiceMBean> buildMBeans() {
        List<RollupServiceMBean> rollupServices = new ArrayList<RollupServiceMBean>();
        for (String resource : System.getProperty("BLUEFLOOD_JMX_LIST").split(",", -1)) {
            try {
                RollupServiceMBean bean = getRollupService(resource);
                bean.setKeepingServerTime(false);
                bean.setPollerPeriod(1000L);
                rollupServices.add(bean);
            } catch (Throwable mostlyExpected) {
                System.out.println(mostlyExpected.getMessage());
            }
        }
        return rollupServices;
    }
    public static void main(String[] args) {
        // Mon Apr 30 07:29:52 PDT 2012
        final long startDate = 1335796192139L / 1000;
        final int days = 30;
        final int hours = 24;
        final long endDate = startDate + 60 * 60 * hours * days; // 30 days from now.
        
        // connect to the RollupService since we're going to be doing a bunch of forced operations on it.
        final Collection<RollupServiceMBean> rollupServices = buildMBeans();
        
        final AtomicLong time = new AtomicLong(startDate);
        
        // cool.  Now we can start sending it some metrics.
        final boolean testing = false;
        Thread telescopeSender = new Thread("Telescope Sender") {
            long counter = 0;
            
            private ScribeClient clientA() {
                final ScribeClient clientA = new ScribeClient("127.0.0.1", 2466) {
                    @Override
                    public boolean log(List<LogEntry> msgs) throws ConnectionException, LogException {
                        if (testing)
                            return true;
                        else
                            return super.log(msgs);
                    }
                };
                return clientA;
            }
            
            private ScribeClient clientB() {
                final ScribeClient clientB = new ScribeClient("127.0.0.1", 3466) {
                    @Override
                    public boolean log(List<LogEntry> msgs) throws ConnectionException, LogException {
                        if (testing)
                            return true;
                        else
                            return super.log(msgs);
                    }
                };
                return clientB;
            }
            
            private static final int batchSize = 2;
            
            public void run() {
                final ArrayList<String> checks = Lists.newArrayList("327", "32", "184", "435"); // shard to 0, 1, 2, 3.
                final int numWriteEndpoints = System.getProperty("blast_at_two") == null ? 1 : 2;
                List<LogEntry> entries = new ArrayList<LogEntry>(3);
                Map<String, Number> metrics = new HashMap<String, Number>();
                metrics.put("dim0.20min", 0);
                metrics.put("dim0.1h", 0);
                metrics.put("dim0.4h", 0);
                metrics.put("dim0.rand10", 0);
                metrics.put("dim0.rand100", 0);
                metrics.put("dim0.sine1h", 0);
                metrics.put("dim0.sine4h", 0);
                Random rand = new Random(System.currentTimeMillis());
                int min20 = 0, min60 = 0, min240 = 0;
                ScribeClient clientA = clientA();
                ScribeClient clientB = clientB();
                Function<Double> sine1h = new SineFunction.DoubleFunction(time.get(), 3600000, 10, -10);
                Function<Double> sine4h = new SineFunction.DoubleFunction(time.get(), 3600000*4, 10, -10);
                
                long lastFlash = 0;
                int logCount = 0;
                while (time.get() < endDate) { 
                    
                    // batch them out somewhat.
                    for (int i = 0; i < batchSize; i++) {
                        long now = time.get();
                        counter++;
                        if (counter % 40 == 0)
                            min20 = 0;
                        if (counter % 120 == 0)
                            min60 = 0;
                        if (counter % 480 == 0)
                            min240 = 0;
                        
                        metrics.put("dim0.20min", min20);
                        metrics.put("dim0.1h", min60);
                        metrics.put("dim0.4h", min240);
                        metrics.put("dim0.rand10", rand.nextInt(10));
                        metrics.put("dim0.rand100", rand.nextInt(100));
                        metrics.put("dim0.sine1h", sine1h.get(now * 1000));
                        metrics.put("dim0.sine4h", sine4h.get(now * 1000));
                        entries.add(new LogEntry("CATEGARY", makeStringTelescope(now * 1000, "acBBBBBBBB", "enBBBBBBBB", checks.get(rand.nextInt(checks.size())), null, metrics)));
                        time.set(now + 30000);
                        
                        min20++;
                        min60++;
                        min240++;
                        try {
                            if (entries.size() >= batchSize) {
                                ScribeClient client = logCount % 2 == 0 || numWriteEndpoints == 1 ? clientA : clientB;
                                boolean ok = client.log(entries);
                                if (!ok)
                                    System.err.println("OUCH " + client.toString());
                                entries.clear();
                                logCount++;
                            }
                        } catch (ConnectionException e) {
                            throw new RuntimeException(e);
                        } catch (LogException e) {
                            clientA = clientA();
                            clientB = clientB();
                            rollupServices.clear();
                            rollupServices.addAll(buildMBeans());
                        }
                        
                        while (true) {
                            try {
                                for (RollupServiceMBean bean : rollupServices)
                                    bean.setServerTime(time.get());
                                break;
                            } catch (Throwable any) {
                                System.err.println("PROBLEM JMXING " + any.getMessage());
                                rollupServices.clear();
                                rollupServices.addAll(buildMBeans());
                            }
                        }
                    }
                    if (time.get() - lastFlash > 3600000) {
                        System.out.println(new java.util.Date(time.get() * 1000));
                        lastFlash = time.get();
                    }
                }
                // set time to way in the future to force final rollups.
                for (RollupServiceMBean bean : rollupServices)
                    bean.setServerTime(time.get() + 600000);
            }
        };
        
        telescopeSender.start();
    }
    
    static RollupServiceMBean getRollupService(String hostAndPort)
    throws MalformedObjectNameException, IOException {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + hostAndPort + "/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        ObjectName objectName = new ObjectName("com.cloudkick.blueflood.service:type=RollupService");
        return JMX.newMBeanProxy(mbsc, objectName, RollupServiceMBean.class);
    }
    
    static <V> String makeStringTelescope(long timeMillis, String account, String entity, String check, String monitoringZone, Map<String, V> metrics) {
        Telescope t = makeTelescope("te12345678", check, account, "http", entity, "www.dusbabek.org", timeMillis, metrics);
        if (monitoringZone != null && !monitoringZone.isEmpty())
            t.setMonitoringZoneId(monitoringZone);
        TelescopeOrRemove tor = new TelescopeOrRemove();
        tor.setTelescope(t);
        try {
            byte[] buf = serializer.serialize(tor);
            return DatatypeConverter.printBase64Binary(buf);
        } catch (TException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private static <V> Telescope makeTelescope(String id, String checkId, String acctId, String checkModule, String entityId, String target, long timestamp, Map<String, V> metrics) {
        Telescope tel = new Telescope(id, checkId, acctId, checkModule, entityId, target, timestamp, 1, VerificationModel.ONE);
        tel.setMetrics(makeMetrics("dim0", metrics));
        return tel;
    }
    
    private static <V> Map<String, Metric> makeMetrics(String dimension, Map<String, V> map) {
        Map<String, Metric> metrics = new HashMap<String, Metric>();
        if (map.isEmpty()) {
            return metrics;
        }
        for (Map.Entry<String, V> entry : map.entrySet()) {
            V obj = entry.getValue();
            metrics.put(entry.getKey(), Util.createMetric(obj));
        }

        V value = map.entrySet().iterator().next().getValue();
        if (value instanceof Number) {
            metrics.put(dimension + ".constint", Util.createMetric(new Integer(2932)));
            metrics.put(dimension + ".constdbl", Util.createMetric(new Double(1.21d)));
        } else if (value instanceof String) {
            metrics.put(dimension + ".conststr0", Util.createMetric(new String("meh")));
            metrics.put(dimension + ".conststr1", Util.createMetric(new String("meow")));
        }

        return metrics;
    }
}
