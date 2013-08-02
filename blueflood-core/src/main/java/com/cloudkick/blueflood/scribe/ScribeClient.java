package com.cloudkick.blueflood.scribe;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import java.util.List;

public class ScribeClient {
    private static final Logger log = LoggerFactory.getLogger(ScribeClient.class);
    
    private final String host;
    private final int port;
    private final scribe.Client client;
    private final TSocket socket;
    private final TFramedTransport transport;
    
    public ScribeClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.socket = new TSocket(host, port);

        transport = new TFramedTransport(this.socket);
        TProtocol protocol = new TBinaryProtocol(transport, false, false);
        client = new scribe.Client(protocol);
    }
    
    public boolean log(List<LogEntry> msgs) throws ConnectionException, LogException {
        try {
            if (!transport.isOpen()) {
                log.info("Connecting to Scribe at {}:{}", host, port);
                transport.open();
            }
        } catch (TTransportException ex) {
            throw new ConnectionException("Could not establish connection with scribe");
        }
        
        try {
            if (client.Log(msgs).equals(ResultCode.OK)) {
                log.debug("Wrote " + msgs.size() + " messages to Scribe");
                return true;
            } else {
                return false;
            }
        } catch (TException ex) {
            transport.close();
            throw new LogException(ex.getMessage(), ex);
        }
    }
    
    public void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }

        if (socket != null && socket.isOpen()) {
            socket.close();
        }
    }
    
    public String toString() {
        return host + ":" + port;
    }
}
