package com.rackspacecloud.blueflood.service.udp;

import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test class whose purpose is to hurl random metrics an a UDP ingestion endpoint.
 * I used netty's UDP APIs.  They were cumbersome.  If this is to serve as an example, we might use a simpler way of
 * setting up the sockets.
 */
public class Broadcaster {

    private static final Logger log = LoggerFactory.getLogger(MainIngestor.class);

    private static final TimeValue TTL = new TimeValue(30, TimeUnit.DAYS);
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Random rand = new Random(75323);

    public void sendHerd(final InetSocketAddress addr, final int numPerSecond) {

        final int id = counter.incrementAndGet();
        final Timer timer = new Timer("UDP Broacast Timer " + id);
        final long period = 1000 / numPerSecond;
        final EventLoopGroup group = new NioEventLoopGroup();

        // My locator will be tentant_${id},udp_int_metric.
        final Locator locator = Locator.createLocatorFromPathComponents("tenant_" + id, "udp_int_metric");
        log.info("Sending metrics for {} starting {}", locator, System.currentTimeMillis());

        try {

            // netty UDP boilerplate.
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                            System.err.println("SHOULD NOT BE GETTING MESSAGES");
                        }
                    });

            final Channel ch = bootstrap.bind(0).sync().channel();

            // we'll send the datagrams using a timer.
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        ch.writeAndFlush(
                            new DatagramPacket(Unpooled.copiedBuffer(UDPMetricSerialization.toBytes(nextMetrics(locator, rand.nextInt(20) + 1))), addr)
                        ).sync();
                    } catch (Exception ex) {
                        ex.printStackTrace(System.err);
                    }
                }

                @Override
                public boolean cancel() {
                    group.shutdownGracefully();
                    return true;
                }
            };

            // set things in motion.
            timer.schedule(task, 0, period);

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    // create a random int metric.
    private static Collection<Metric> nextMetrics(Locator locator, int count) {
        List<Metric> metrics = new ArrayList<Metric>(count);
        for (int i = 0; i < count; i++) {
            metrics.add(new Metric(locator, rand.nextInt(1024), System.currentTimeMillis(), TTL, "gigawatts"));
        }
        log.info("Generating metrics for {} until {}", locator, System.currentTimeMillis());
        return metrics;
    }

    // for testing I'm using localhost:2525.
    public static void main(String args[]) {
        String host = Configuration.getInstance().getStringProperty("UDP_BIND_HOST");
        int port = Configuration.getInstance().getIntegerProperty("UDP_BIND_PORT");
        InetSocketAddress destination = new InetSocketAddress(host, port);
        Broadcaster broadcaster = new Broadcaster();

        broadcaster.sendHerd(destination, 1);
    }
}
