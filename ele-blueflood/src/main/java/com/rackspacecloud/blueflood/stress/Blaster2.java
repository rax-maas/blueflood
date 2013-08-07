package com.rackspacecloud.blueflood.stress;

import com.rackspacecloud.blueflood.scribe.ConnectionException;
import com.rackspacecloud.blueflood.scribe.LogException;
import com.rackspacecloud.blueflood.scribe.ScribeClient;
import com.google.common.base.Joiner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.thrift.transport.TTransportException;
import scribe.thrift.LogEntry;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Blaster2 implements Blaster2MBean, Runnable {
    private static final int RELOAD_SPREAD_SECONDS = 30;
    private static final Random random = new Random(System.currentTimeMillis());
    public static final Function<Number>[] functions = new Function[] {
            new RandomFunction.IntFunction(1000),
            new RandomFunction.IntFunction(100),
            new RandomFunction.DoubleFunction(50),
            new RandomFunction.DoubleFunction(150),
            new SawtoothFunction.IntFunction(System.currentTimeMillis() / 1000, 3600), // 1 hr
            new SawtoothFunction.IntFunction(System.currentTimeMillis() / 1000, 3600 * 6), // 6hrs.
            new SawtoothFunction.IntFunction(System.currentTimeMillis() / 1000, 3600 * 12), // 12hrs.
            new SineFunction.DoubleFunction(System.currentTimeMillis() / 1000, 1000 * 3600 * 4, 100, -100), // 4 hrs
            new SineFunction.DoubleFunction(System.currentTimeMillis() / 1000, 1000 * 3600 * 12, 100, -100), // 12 hrs
            new SineFunction.DoubleFunction(System.currentTimeMillis() / 1000, 1000 * 3600 * 48, 100, -100), // 48 hrs
            new SineFunction.DoubleFunction(System.currentTimeMillis() / 1000, 1000 * 3600, 100, -100), // 1 hrs
            new ConstantFunction.DoubleFunction(random.nextDouble() * 100),
            new ConstantFunction.IntFunction(random.nextInt(1000))
    };
    
    private int metricsPerCheck = 1;
    private String[] writeSlaves = new String[] {};
    private final Joiner slaveJoiner = Joiner.on(",");
    private boolean running = false;
    
    private final ArrayList<TelescopeGenerator> telescopes;
    private final ArrayList<PushThread> pushers;
    private final AtomicLong pushCounter;
    private final AtomicLong queueCounter;
    private final Counter scribeErrorCount;
    private final Timer pushTimer;
    
    Blaster2(ArrayList<TelescopeGenerator> telescopes, ArrayList<PushThread> pushers) {
        this.telescopes = telescopes;
        this.pushers = pushers;
        
        // register the mbean.
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final String name = String.format("com.rackspacecloud.blueflood.stress:type=%s", Blaster2.class.getSimpleName());
            final ObjectName nameObj = new ObjectName(name);
            mbs.registerMBean(this, nameObj);
        } catch (Exception exc) {
            exc.printStackTrace();
            System.exit(-1);
        }
        pushTimer = Metrics.newTimer(Blaster2.class, "Push Timer", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        pushCounter = new AtomicLong(0);
        queueCounter = new AtomicLong(0);
        scribeErrorCount = Metrics.newCounter(Blaster2.class, "Scribe Errors");
    }
    
    public void run() {
        while (true) {
            if (isBlasting()) {
                long now = System.currentTimeMillis() / 1000;
                synchronized (telescopes) {
                    for (Blaster2.TelescopeGenerator gen : telescopes) {
                        if (now - gen.getLastSent() >= 30) {
                            //System.out.println(String.format("queuing %s at %s", gen.checkName, new Date(now * 1000).toString()));
                            String nextData = gen.nextTelescope(now);
                            boolean pushed = false;
                            while (!pushed) {
                                // make sure we have a thread to push to.
                                while (pushers.size() < 1)
                                    setNumThreads(1);    
                                int nextPusherIndex = random.nextInt(pushers.size());
                                pushed = pushers.get(nextPusherIndex).put(nextData);
                                if (!pushed) {
                                    // remove and recreate
                                    System.out.println("Replacing bad pusher");
                                    PushThread pushThread = pushers.remove(nextPusherIndex);
                                    pushThread.quit();
                                    PushThread newPushthread = new PushThread(this, new LinkedList<String>(pushThread.messages));
                                    newPushthread.start();
                                }
                            }
                            gen.setLastSent(now);
                        }
                    }
                }
            }
            try { Thread.currentThread().sleep(100L); } catch (Exception ex) {}
        }
    }

    public void addChecks(int num) {
        for (int i = 0; i < num; i++)
            setNumChecks(getNumChecks() + 1);
    }

    public void setWriteSlaves(String s) {
        writeSlaves = s.split(",", -1);
    }

    public String getWriteSlaves() {
       return slaveJoiner.join(writeSlaves);
    }

    public void setNumChecks(int i) {
        synchronized (telescopes) {
            i = Math.max(1, i);
            while (telescopes.size() != i) {
                if (i > telescopes.size())
                    telescopes.add(new TelescopeGenerator("ch" + randString(8), getMetricsPerCheck()));
            }
        }
    }

    public int getNumChecks() {
        return telescopes.size();
    }

    public void setMetricsPerCheck(int i) {
        i = Math.max(1, i);
        metricsPerCheck = i;
    }

    public int getMetricsPerCheck() {
        return metricsPerCheck;    
    }

    public void setNumThreads(int i) {
        i = Math.max(0, i);
        while (pushers.size() != i) {
            if (i > pushers.size()) {
                PushThread th = new PushThread(this);
                pushers.add(th);
                th.start();
            } else if (i < pushers.size())
                pushers.remove(0).quit();
        }
    }

    public int getUnpushedMetricCount() {
        int count = 0;
        for (PushThread thread : pushers)
            count += thread.messages.size();
        return count;
    }

    public int getNumThreads() {
        return pushers.size();
    }

    public void go() {
        running = true;
    }

    public void stop() {
        running = false;
    }

    public boolean isBlasting() {
        return running;
    }

    public int getTotalChecks() {
        return telescopes.size();
    }

    public long getTotalEnqueued() {
        return queueCounter.get();
    }

    public long getTotalLogged() {
        return pushCounter.get();
    }

    public void dumpStrings() {
        synchronized (telescopes) {
            for (TelescopeGenerator gen : telescopes) {
                System.out.println(gen.checkName);
                for (String metric : gen.metricGenerators.keySet())
                    System.out.println("  " + metric);
            }
        }
    }

    public void save(String path) throws IOException {
        synchronized (telescopes) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(path, false));
            for (TelescopeGenerator gen : telescopes) {
                writer.write(gen.toString());
                writer.newLine();
            }
            writer.close();
        }
    }

    public void load(String path) throws IOException {
        final List<TelescopeGenerator> toAdd = new ArrayList<TelescopeGenerator>();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = reader.readLine();
        while (line != null && line.length() > 0) {
            try {
                TelescopeGenerator gen = TelescopeGenerator.fromString(line);
                toAdd.add(gen);
            } catch (RuntimeException ex) {
                ex.printStackTrace();
                throw ex;
            }
            line = reader.readLine();
        }
        reader.close();
        
        // now that they are all read in, apply them, but gradually. space them out over RELOAD_SPREAD_SECONDS.
        int count = 0;
        long start = System.currentTimeMillis();
        for (TelescopeGenerator gen : toAdd) {
            synchronized (telescopes) {
                telescopes.add(gen);  
            }
            count += 1;
            // all this math is to ensure that checks are aded more or less evenly spaced over RELOAD_SPREAD_SECONDS.
            long now = System.currentTimeMillis();
            int expectedCount = (toAdd.size() / RELOAD_SPREAD_SECONDS) * (((int)(now - start)) / 1000);
            if (count > expectedCount)
                try { Thread.currentThread().sleep(100); } catch (Exception ex) {}
            System.out.println(String.format("Loaded %d of %d checks", count, toAdd.size()));
        }
    }

    public void addOverTime(final int checkPerSecond, final int numSeconds) {
        new Thread() {
            public void run() {
                for (int i = 0; i < numSeconds; i++) {
                    System.out.println(String.format("%s added over time %d of %d", getName(), (i+1)*checkPerSecond, checkPerSecond*numSeconds));
                    long start = System.currentTimeMillis();
                    addChecks(checkPerSecond);
                    try { sleep(Math.max(1, 1000 - System.currentTimeMillis() + start)); } catch (Exception ex) {}
                }
            }
        }.start();
    }

    private static final char[] dict = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqurstuvwxyz0123456789_".toCharArray();
    static String randString(int sz) {
        StringBuilder sb = new StringBuilder();
        while (sz-- > 0)
            sb.append(dict[random.nextInt(dict.length)]);
        return sb.toString();
    }
    
    private static int clientPos = 0; 
    private static synchronized ScribeClient nextClient(Blaster2MBean bean) {
        String[] allHosts = bean.getWriteSlaves().split(",", -1);
        if (clientPos >= allHosts.length)
            // this means that the number of hosts has contracted.
            clientPos = 0;
        String[] parts = allHosts[clientPos].split(":", -1);
        clientPos = (clientPos + 1) % allHosts.length;
        System.out.println("Connecting " + allHosts[clientPos]);
        return new ScribeClient(parts[0], Integer.parseInt(parts[1]));
    }
    
    static class TelescopeGenerator {
        private static final int version = 1; // used for serialization.
        private final Map<String, Integer> metricGenerators;
        private final String checkName;
        long lastSent = 0;
        
        private TelescopeGenerator(String checkName) {
            this.checkName = checkName;
            metricGenerators = new HashMap<String, Integer>();
        }
        
        TelescopeGenerator(String checkName, int numMetrics) {
            this(checkName);
            for (int i = 0; i < numMetrics; i++) {
                int next = random.nextInt(functions.length);
                metricGenerators.put(functions[next].getClass().getName().substring(31) + "_" + randString(8), next);
            }
        }
        
        private Map<String, Number> makeMetrics(long secs) {
            Map<String, Number> map = new HashMap<String, Number>();
            for (Map.Entry<String, Integer> entry : metricGenerators.entrySet())
                map.put(entry.getKey(), functions[entry.getValue()].get(secs));
            return map;
        }
        
        String nextTelescope(long secs) {
            return Blaster.makeStringTelescope(secs * 1000, "acCCCCCCCC", "enCCCCCCCC", checkName, null, makeMetrics(secs));
        }
        
        public long getLastSent() { return lastSent; }
        public void setLastSent(long l) { lastSent = l; }
        
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb = sb.append(version).append(",");
            sb = sb.append(checkName).append(",");
            sb = sb.append(lastSent).append(",");
            sb = sb.append(metricGenerators.size()).append(",");
            for (Map.Entry<String, Integer> entry : metricGenerators.entrySet())
                sb = sb.append(entry.getKey()).append(",").append(entry.getValue()).append(",");
            return sb.toString();
        }
        
        static TelescopeGenerator fromString(String s) {
            int index = 0;
            String[] parts = s.split(",", -1);
            int sversion = Integer.parseInt(parts[index++]);
            String checkName = parts[index++];
            long lastSent = Long.parseLong(parts[index++]);
            int numGenerators = Integer.parseInt(parts[index++]);
            
            TelescopeGenerator gen = new TelescopeGenerator(checkName);
            gen.lastSent = lastSent;
            for (int i = 0; i < numGenerators; i++) {
                String funcName = parts[index++];
                int funcIndex = Integer.parseInt(parts[index++]);
                gen.metricGenerators.put(funcName, funcIndex);
            }
            return gen;
        }
    }
    
    
    private static final AtomicInteger pushThreadIdFactory = new AtomicInteger(0);
    
    static class PushThread extends Thread {
        private boolean isQuitting = false;
        private final Queue<String> messages;
        private ScribeClient client;
        private long connectedAt = 0;
        private final Blaster2 _bean;
        private long lastWriteCompletedAt = 0;
        
        PushThread(Blaster2 bean, Queue<String> messages) {
            super("Push Thread " + pushThreadIdFactory.incrementAndGet());
            this._bean = bean;
            this.messages = messages;
        }
        
        PushThread(Blaster2 bean) {
            this(bean, new LinkedList<String>());
        }
        
        void quit() {
            isQuitting = true;
            interrupt();
        }
        
        boolean put(String msg) {
            if (System.currentTimeMillis() - lastWriteCompletedAt > 5000 && lastWriteCompletedAt > 0)
                return false;
            this._bean.queueCounter.incrementAndGet();
            messages.add(msg);
            return true;
        }
        
        public void run() {
            client = nextClient(_bean);
            connectedAt = System.currentTimeMillis();
            while (!isQuitting) {
                // grab and push.
                if (System.currentTimeMillis() - connectedAt > 5000) {
                    client.close();
                    client = nextClient(_bean);
                    connectedAt = System.currentTimeMillis();
                }
                List<LogEntry> entries = new ArrayList<LogEntry>();
                while (entries.size() < 30 && messages.size() > 0) {
                    try {
                        entries.add(new LogEntry("CATEGARY", messages.remove()));
                    } catch (NoSuchElementException ex) {
                        // I don't know why this happens periodically. this is the only place that consumes from the queue.
                        // in the case of a recovered thread, the queue is copied, so nothing else is removing from it.
                        // I suspect there is some kind of race in LinkedList between add() and remove() where size() 
                        // reports and element that cannot be removed.
                        System.err.println(ex.getMessage());
                    }
                }
                if (entries.size() > 0) {
                    int tries = 10;
                    while (tries-- > 0) {
                        TimerContext ctx = this._bean.pushTimer.time();
                        try {
                            long start = System.currentTimeMillis();
                            client.log(entries);
                            lastWriteCompletedAt = System.currentTimeMillis();
                            System.out.println(String.format("%s sent %d in %d to %s", getName(), entries.size(), lastWriteCompletedAt-start, client.toString()));
                            this._bean.pushCounter.addAndGet(entries.size());
                            break;
                        } catch (ConnectionException e) {
                            _bean.scribeErrorCount.inc();
                            throw new RuntimeException(e);
                        } catch (LogException ex) {
                            client.close();
                            _bean.scribeErrorCount.inc();
                            if (ex.getCause() instanceof TTransportException)
                                System.out.println(String.format("ERROR TYPE: %d, Thread: %s, Host: %s", ((TTransportException)ex.getCause()).getType(), getName(), client.toString()));
                            else
                                System.out.println(ex.getMessage());
                            client = nextClient(_bean);
                        } finally {
                            ctx.stop();
                        }
                    }
                    if (tries <= 0)
                        System.out.println("Failed to send anything to blueflood");
                } else {
                    try { sleep(500L); } catch (InterruptedException ex) {}
                }
            }
            System.out.println("Thread is done");
        }
    }
}
