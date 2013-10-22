package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.statsd.containers.Stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class DoesStuff {
    
    private static final Timer valueTimer = new Timer("Metric Append Timer", false);
    
    private static StatsdClient client;

    /**
     * usage:
     * 
     * java DoesStuff localhost,8125,1500 counter,counter0,0.75,100 timer,timer0,100 intgauge,igauge0,100,5000 doublegauge,dgauge0,100,100.0 incgauge,incgauge0,100
     */
    public static void main(String args[]) {
        // host, port, buffer size
        String[] clientParts = args[0].split(",", -1);
        try {
            client = new StatsdClient(
                    clientParts[0], 
                    Integer.parseInt(clientParts[1]), 
                    Integer.parseInt(clientParts[2]));
        } catch (Exception ex) {
            System.err.println(ex);
            System.exit(-1);
        }
        
        // abusing this method.
        String[] descriptorLines = Stat.Parser.shiftLeft(args, 1);
        scheduleThings(descriptorLines);
        
        
    }
    
    // descriptor lines are comma delimited: type,name,param0,param1,...,paramN
    public static void scheduleThings(String descriptorLines[]) {
        Collection objects = new ArrayList();
        for (String arg : descriptorLines) {
            String[] parts = arg.split(",", -1);
            if ("counter".equals(parts[0])) {
                objects.add(new Counter(
                        parts[1],
                        Float.parseFloat(parts[2]),
                        Long.parseLong(parts[3])
                ));
            } else if ("timer".equals(parts[0])) {
                objects.add(new STimer(
                        parts[1],
                        Long.parseLong(parts[2])
                ));
            } else if ("intgauge".equals(parts[0])) {
                objects.add(new SRandomInt(
                        parts[1],
                        Long.parseLong(parts[2]),
                        Integer.parseInt(parts[3])
                ));
            } else if ("doublegauge".equals(parts[0])) {
                objects.add(new SRandomDouble(
                        parts[1],
                        Long.parseLong(parts[2]),
                        Double.parseDouble(parts[3])
                ));
            } else if ("incgauge".equals(parts[0])) {
                objects.add(new IncrementingGauge(
                        parts[1],
                        Long.parseLong(parts[2])
                ));
            }
        }
    }
    
    
    private static class Counter {
        private final Random random = new Random(System.currentTimeMillis());
        
        Counter(final String name, final float incChance, long period) {
            valueTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (random.nextFloat() <= incChance)
                        System.out.println("incrementing " + name);
                        client.increment(name, 1);
                }
            }, 0, period);
        }
    }
    
    private static class STimer {
        private final Random random = new Random(System.currentTimeMillis());
        
        STimer(final String name, long period) {
            valueTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    int value = Math.abs((int) (random.nextGaussian() * 10d));
                    System.out.println(String.format("timing %s %s", name, value));
                    client.timer(name, value);
                }
            }, 0, period);
        }
    }
    
    private static class SRandomInt {
        private final Random random = new Random(System.currentTimeMillis());
        
        SRandomInt(final String name, long period, final int max) {
            valueTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    int value = random.nextInt(max);
                    System.out.println(String.format("irand %d", value));
                    client.gauge(name, value);
                }
            }, 0, period);
        }
    }
    
    private static class SRandomDouble {
        private final Random random = new Random(System.currentTimeMillis());
        
        SRandomDouble(final String name, long period, final double multiplier) {
            valueTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    double value = random.nextDouble() * multiplier;
                    System.out.println(String.format("drand %s", value));
                    client.gauge(name, value);
                }
            }, 0, period);
        }
    }
    
    private static class IncrementingGauge {
        private int value = 0;
        
        IncrementingGauge(final String name, long period) {
            valueTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    int i = value++;
                    System.out.println(String.format("igauge %d", i));
                    client.gauge(name, i);
                }
            }, 0, period);
        }
    }
}
