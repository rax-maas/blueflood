package com.rackspacecloud.blueflood.service.udp;

import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Nothing Blueflood-related happens in this class. It simple binds to a UDP port and forwards datagrams to the
 * AsyncChain that has been designated to handle them.
 */
public class UdpListenerService extends SimpleChannelInboundHandler<DatagramPacket> {
    
    private static final Logger log = LoggerFactory.getLogger(UdpListenerService.class); 
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SocketAddress addr;
    
    private AsyncChain<DatagramPacket, ?> chain = null;
    
    public UdpListenerService(SocketAddress addr) {
        this.addr = addr;
    }
    
    public UdpListenerService withProcessor(AsyncChain<DatagramPacket, ?> chain) {
        this.chain = chain;
        return this;
    }
    
    private void run() {
        if (running.get())
            return;
        running.set(true);
        
        Thread thread = new Thread("UDP Network Listener") {
            public void run() {
                try {
                    EventLoopGroup group = new NioEventLoopGroup();
                    try {
                        Bootstrap bootstrap = new Bootstrap()
                                .group(group)
                                .channel(NioDatagramChannel.class)
                                .handler(UdpListenerService.this);
                        
                        // remember folks, this is blocking.
                        log.info(String.format("Binding to %s", addr.toString()));
                        bootstrap.bind(addr).sync().channel().closeFuture().await();
                        
                    } finally {
                        group.shutdownGracefully();
                        running.set(false);
                    }
                } catch (Exception ex) {
                    running.set(false);
                    log.error(ex.getMessage(), ex);
                    System.exit(StopReasons.CANNOT_START_UDP_LISTENER);
                }
            }
        };
        thread.start();
    }
    
    public synchronized void start() {
        if (running.get())
            return;
        else {
            run();
            // running.get == true now.
        }
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
        if (chain == null)
            log.warn("No processor chain set to receive packets. Dropping.");
        else {
            msg.content().retain();
            // technically, the chain returns a Future, but we don't really care about that.
            chain.apply(msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage(), cause);
    }
}
