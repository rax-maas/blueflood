package com.rackspacecloud.blueflood.tools.jmx;

import javax.management.ObjectName;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiQuery {
    private final String[] hosts;
    private final String[] attributes;
    private final ObjectName object;
    
    public MultiQuery(String[] hosts, String[] attributes, ObjectName object) {
        this.hosts = hosts;
        this.attributes = attributes;
        this.object = object;
    }
    
    public Collection<OutputFormatter> assembleBlocking() throws InterruptedException {
        final List<OutputFormatter> output = new ArrayList<OutputFormatter>();
        ExecutorService executor = Executors.newFixedThreadPool(hosts.length);
        final CountDownLatch waitLatch = new CountDownLatch(hosts.length);
        
        for (String host : hosts) {
            final HostAndPort hostInfo = HostAndPort.fromString(host);
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        if (JmxQuery.DEBUG) {
                            synchronized (JmxQuery.class) {
                                System.out.println("Checking " + hostInfo.getHost());
                            }
                        }
                        output.add(new OutputFormatter(hostInfo, new FetchAttributesCallable(hostInfo, object, attributes).call()));
                    } catch (Exception any) {
                        if (JmxQuery.DEBUG)
                            System.err.println(any.getMessage());
                            //any.printStackTrace();
                        String[] result = new String[attributes.length];
                        for (int i = 0; i < result.length; i++)
                            result[i] = "ERROR";
                        output.add(new OutputFormatter(hostInfo, result));
                    } finally {
                        waitLatch.countDown();
                    }
                }
            });
        }
        waitLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        return output;
    }
    
    public void print(Collection<OutputFormatter> output, PrintStream out) throws InterruptedException {
        String[] headers = new String[attributes.length + 1];
        headers[0] = "Host";
        System.arraycopy(attributes, 0, headers, 1, attributes.length);
        OutputFormatter[] outputArr = output.toArray(new OutputFormatter[output.size()]);
        int[] max = OutputFormatter.computeMaximums(headers, outputArr);
        String[] prettyResults = OutputFormatter.format(max, outputArr);
        String prettyHeader = OutputFormatter.formatHeader(max, headers);
        
        out.println(prettyHeader);
        for (String prettyResult : prettyResults)
            out.println(prettyResult); 
    }
}
