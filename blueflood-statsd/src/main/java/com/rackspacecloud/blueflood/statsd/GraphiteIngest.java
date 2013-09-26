package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for a graphite ingestion point.  This thing receives bundles from statsd.
 */
public class GraphiteIngest {
    private static final Logger log = LoggerFactory.getLogger(GraphiteIngest.class);
    
    private final InetSocketAddress iface;
    
    public GraphiteIngest(InetSocketAddress iface) {
        this.iface = iface;
    }
    
    // set up the socket interface and go.
    private void run(final AsyncChain<List<ByteBuf>, Object> processor) throws Exception {
        EventLoopGroup parentGroup = new NioEventLoopGroup();
        EventLoopGroup childGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(parentGroup, childGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        
                        // remember, this gets called once per socket connection.
                        // connections from the 
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new GraphiteHandler(processor));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    // consider setting SO_LINGER to something small on the child.
                    .childOption(ChannelOption.SO_KEEPALIVE, false); // because statsd doesn't reuse sockets.
            
            ChannelFuture future = bootstrap.bind(iface).sync();
            future.channel().closeFuture().sync();
        } finally {
            childGroup.shutdownGracefully();
            parentGroup.shutdownGracefully();
        }
    }

    /**
     * This thing handles the bundles. Basically, it sends buffers into an ingestion pipeline, not unlike what you see
     * in other places. 
     */
    @ChannelHandler.Sharable
    private static class GraphiteHandler extends ChannelInboundHandlerAdapter {

        private final AsyncChain<List<ByteBuf>, Object> processor;
        private final List<ByteBuf> buffers = new ArrayList<ByteBuf>();
        
        public GraphiteHandler(AsyncChain<List<ByteBuf>, Object> processor) {
            this.processor = processor;    
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error(cause.getMessage(), cause);
            ctx.close();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            buffers.add((ByteBuf)msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            super.channelReadComplete(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            super.handlerRemoved(ctx);
            processor.apply(buffers);
        }
    }
    
    public static void main(String args[]) {
        try {
            Configuration.init();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            System.exit(-1);
        }
        
        String bindAddr = getListenAddress();
        int bindPort = getListenPort();
        
        try {
            // set up an async chain to group messages.
            AsyncChain<List<ByteBuf>, Object> processor = new AsyncChain<List<ByteBuf>, Object>()
                    .withFunction(new StringListBuilder(new ThreadPoolBuilder()
                        .withName("String Constructor")
                        .withUnboundedQueue()
                        .withCorePoolSize(5)
                        .withMaxPoolSize(5)
                        .build()))
                    .withFunction(new StatParser(new ThreadPoolBuilder()
                        .withCorePoolSize(5)
                        .withMaxPoolSize(5)
                        .withUnboundedQueue()
                        .withName("Stat Parser")
                        .build()))
                    ;
            
            new GraphiteIngest(new InetSocketAddress(bindAddr, bindPort)).run(processor);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            System.exit(-1);
        }
    }
    
    // todo: these don't belong in Configuration.java, but they belong somewhere other than here. 
    
    private static String getListenAddress() {
        String addr = Configuration.getStringProperty("GRAPHITE_INGEST_ADDRESS");
        if (addr == null)
            addr = "127.0.0.1"; // sorry, folks.
        return addr;
    }
    
    private static int getListenPort() {
        String portStr = Configuration.getStringProperty("GRAPHITE_INGEST_PORT");
        if (portStr == null)
            portStr = "8126";
        return Integer.parseInt(portStr);
    }
}
