package com.rackspacecloud.blueflood.statsd;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Simple client inspired by the one that ships with statsd which is (C) 2011 Meetup, Inc., written by Andrew 
 * Gwozdziewycz <andrew@meetup.com>, @apgwoz
 * 
 * This one doesn't do any of the aggregated sends, or percentile sends.
 **/
public class StatsdClient {
    
    private static final Logger log = LoggerFactory.getLogger(StatsdClient.class);
    private static final byte EOL = (byte)'\n';
    
    private final SocketAddress addr;
    private final DatagramChannel chan;
    private final ByteBuffer buf;
    
    
    public StatsdClient(String host, int port, int bufferSize) throws Exception {
        this.addr = new InetSocketAddress(host, port);
        this.chan = DatagramChannel.open();
        this.buf = ByteBuffer.allocate(bufferSize);
    }
    
    public boolean increment(String key, int magnitude) {
        String stat = String.format(Locale.ENGLISH, "%s:%d|c", key, magnitude);
        return maybeAppend(stat);
    }
    
    public boolean timer(String key, int value) {
        String stat = String.format(Locale.ENGLISH, "%s:%d|ms", key, value);
        return maybeAppend(stat);
    }
    
    public boolean gauge(String key, double magnitude) {
        String stat = String.format(Locale.ENGLISH, "%s:%s|g", key, magnitude);
        return maybeAppend(stat);
    }
    
    public boolean set(String key, String value) {
        String stat = String.format(Locale.ENGLISH, "%s:%s|s", key, value);
        return maybeAppend(stat);
    }
    
    // this is a single line.
    private boolean maybeAppend(String stat) {
        try {
            byte[] data = stat.getBytes(Charsets.UTF_8);
            if (buf.remaining() < (data.length + 1))
                if (!flush()) {
                    log.error("Problem flushing buffer");
                    return false;
                }
            
            // if this is not the first metric, delineate.
            if (buf.position() > 0)
                buf.put(EOL);
            
            buf.put(data);    
            return true;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return false;   
        }
    } 
    
    public synchronized boolean flush() {
        int sz = buf.position();
        log.info(String.format("flushing %d bytes", sz));
        if (sz <= 0)
            return false; // nothing to send.
        
        buf.flip();
        int sent = 0;
        try {
            sent = chan.send(buf, addr);
            buf.limit(buf.capacity());
            buf.rewind();
        } catch (IOException ex) {
            log.error(String.format("Couldn't write to %s", addr.toString()));
            return false;
        }
        if (sent == sz)
            return true;
        else
            return false;
    }
    
    public static void main(String args[]) {
        final Random rand = new Random(56234363L);
        final List<Integer> set = new ArrayList<Integer>() {{
            for (int i = 0; i < 50; i++) {
                add(i + 100);
            }
        }};
        int counter = 0;
        try {
            StatsdClient client = new StatsdClient("192.168.231.128", 8125, 1500);
            
            while (true) {
                
                // statsd will take these metric names and append "stats.what." where "what" is one of { timers | gauges | 
                client.increment("gary.foo.bar.counter", 1);
                client.timer("gary.foo.bar.timer", rand.nextInt(100));
                client.gauge("gary.foo.bar.gauge", (double)counter);
                if (rand.nextDouble() < 0.05)
                    client.set("gary.foo.bar.set", set.get(rand.nextInt(set.size())).toString());    
                
                Thread.sleep((100));
                counter++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
    
}
