/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.statsd;

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
     * stats_host,statsd_port,statsd_buffer_size [metric_descriptor ]+
     * 
     * A metric_descriptor takes the following formats:
     *  For a counter:              counter,name,increment_chance,period_in_ms
     *  for a timer:                timer,name,period_in_ms
     *  for a random int gauge:     intgauge,name,period_in_ms,max_value
     *  for a random double gauge:  doublegauge,name,period_in_ms,max_value
     *  for an incrementing gauge:  incgauge,name,period_in_ms
     *  
     *  spaces are used to separate metrics, so don't include those in your names, etc.  Here is an example:
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
        String[] descriptorLines = Util.shiftLeft(args, 1);
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
//                        System.out.println("incrementing " + name);
                        client.counter(name, 1);
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
//                    System.out.println(String.format("timing %s %s", name, value));
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
//                    System.out.println(String.format("irand %d", value));
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
//                    System.out.println(String.format("drand %s", value));
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
//                    System.out.println(String.format("igauge %d", i));
                    client.gauge(name, i);
                }
            }, 0, period);
        }
    }
}
