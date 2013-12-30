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
    
    public boolean counter(String key, int magnitude) {
        String stat = String.format("%s:%d|c", key, magnitude);
        return maybeAppend(stat);
    }
    
    public boolean timer(String key, int value) {
        String stat = String.format("%s:%d|ms", key, value);
        return maybeAppend(stat);
    }
    
    public boolean gauge(String key, double magnitude) {
        String stat = String.format("%s:%s|g", key, magnitude);
        return maybeAppend(stat);
    }
    
    public boolean set(String key, String value) {
        String stat = String.format("%s:%s|s", key, value);
        return maybeAppend(stat);
    }
    
    // this is a single line.
    private boolean maybeAppend(String stat) {
        try {
            byte[] data = stat.getBytes(Charsets.UTF_8);
            synchronized (buf) {
                if (buf.remaining() < (data.length + 1))
                    if (!flush()) {
                        log.error("Problem flushing buffer");
                        return false;
                    }
                
                // if this is not the first metric, delineate.
                if (buf.position() > 0)
                    buf.put(EOL);
                
                buf.put(data);
            }
            return true;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return false;   
        }
    } 
    
    public synchronized boolean flush() {
        synchronized (buf) {
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
    }
}
