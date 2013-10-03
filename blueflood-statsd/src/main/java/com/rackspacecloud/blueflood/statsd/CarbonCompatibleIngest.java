package com.rackspacecloud.blueflood.statsd;

import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.utils.TimeValue;
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
import java.util.concurrent.TimeUnit;

/**
 * Main entry point for a graphite ingestion point.  This thing receives bundles from statsd.
 */
public class CarbonCompatibleIngest {
    private static final Logger log = LoggerFactory.getLogger(CarbonCompatibleIngest.class);
    
    private final InetSocketAddress iface;
    
    public CarbonCompatibleIngest(InetSocketAddress iface) {
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

        MetadataCache typeCache = MetadataCache.createLoadingCacheInstance(new TimeValue(24, TimeUnit.HOURS), 5);
        // uncomment if you want to use a TTL cache.
//        TtlCache ttlCache = new TtlCache(
//                "statsd ttl cache",
//                new TimeValue(120, TimeUnit.HOURS),
//                5,
//                InternalAPIFactory.createDefaultTTLProvider()) {
//            // we only care about caching full res values.
//            @Override
//            protected Map<ColumnFamily<Locator, Long>, TimeValue> buildTtlMap(Account acct) {
//                Map<ColumnFamily<Locator, Long>, TimeValue> map = new HashMap<ColumnFamily<Locator, Long>, TimeValue>();
//                map.put(AstyanaxIO.getColumnFamilyMapper().get(Granularity.FULL.name()), acct.getMetricTtl("full"));
//                return map;
//            }
//        };
//        
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
                    .withFunction(new TypeCacher(new ThreadPoolBuilder()
                        .withCorePoolSize(5)
                        .withMaxPoolSize(5)
                        .withUnboundedQueue()
                        .withName("Cache Metric Type")
                        .build(), typeCache))
                    .withFunction(new MetricConverter(new ThreadPoolBuilder()
                        .withCorePoolSize(5)
                        .withMaxPoolSize(5)
                        .withUnboundedQueue()
                        .withName("stat->metric converter")
                        .build()))
                    // uncomment if you want to use a TTL cache.
//                    .withFunction(new TtlAffixer(new ThreadPoolBuilder()
//                        .withCorePoolSize(5)
//                        .withMaxPoolSize(5)
//                        .withUnboundedQueue()
//                        .withName("TTL affixer")
//                        .build(), ttlCache));
                    ;
            
            new CarbonCompatibleIngest(new InetSocketAddress(bindAddr, bindPort)).run(processor);
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
