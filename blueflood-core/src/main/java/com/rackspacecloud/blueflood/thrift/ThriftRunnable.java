package com.rackspacecloud.blueflood.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TBaseProcessor;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

public class ThriftRunnable implements Runnable {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ThriftRunnable.class);

    private TBaseProcessor processor;

    // Transport
    TCustomServerSocket server;

    private String name;
    private Integer port;
    private String host;
    private final int rpcTimeout;
    private final int frameSize;
    private final ExecutorService executorService;

    public ThriftRunnable(String host, int port, String name, TBaseProcessor processor, int rpcTimeout, int frameSize, ExecutorService executorService) {
        this.host = host;
        this.port = port;
        this.name = name;
        this.processor = processor;
        this.rpcTimeout = rpcTimeout;
        this.frameSize = frameSize;
        this.executorService = executorService;
    }

    public void run() {
        log.info("Starting {} server on {}:{}...", new Object[] {this.name, this.host, this.port});

        try
        {
            server = new TCustomServerSocket(new InetSocketAddress(this.host, this.port), true, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
        catch (TTransportException e)
        {
            throw new UnrecoverableException(String.format("Unable to create thrift socket to %s:%s",
                        this.host, this.port), e);
        }

        log.info(String.format("Binding thrift service to %s:%s", this.host, this.port));

        // Protocol factory
        TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(false, false, frameSize);

        // Transport factory
        TTransportFactory inTransportFactory, outTransportFactory;
        inTransportFactory  = new TFramedTransport.Factory(frameSize);
        outTransportFactory = new TFramedTransport.Factory(frameSize);
        log.info("Using TFastFramedTransport with a max frame size of {} bytes.", frameSize);

        // ThreadPool Server
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(server)
                                      .executorService(executorService)
                                      .inputTransportFactory(inTransportFactory)
                                      .outputTransportFactory(outTransportFactory)
                                      .inputProtocolFactory(tProtocolFactory)
                                      .outputProtocolFactory(tProtocolFactory)
                                      .processor(this.processor);

        TThreadPoolServer ttps = new TThreadPoolServer(args);
        ttps.serve();
  }
}
