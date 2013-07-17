package com.cloudkick.blueflood.inputs.handlers;

import com.cloudkick.blueflood.cache.MetadataCache;
import com.cloudkick.blueflood.concurrent.AsyncChain;
import com.cloudkick.blueflood.concurrent.ThreadPoolBuilder;
import com.cloudkick.blueflood.inputs.formats.JSONMetricsContainer;
import com.cloudkick.blueflood.inputs.processors.BatchSplitter;
import com.cloudkick.blueflood.inputs.processors.BatchWriter;
import com.cloudkick.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.cloudkick.blueflood.io.AstyanaxWriter;
import com.cloudkick.blueflood.io.Constants;
import com.cloudkick.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.cloudkick.blueflood.service.ScheduleContext;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.MetricsCollection;
import com.cloudkick.blueflood.http.DefaultHandler;
import com.cloudkick.blueflood.http.HttpRequestHandler;
import com.cloudkick.blueflood.http.RouteMatcher;
import com.cloudkick.blueflood.utils.TimeValue;
import com.google.gson.JsonParseException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.jboss.netty.channel.Channels.pipeline;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpHandler.class);
    private static  TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);
    private static int WRITE_THREADS = 50; // metrics will be batched into this many partitions.
    private static int MAX_CONTENT_LENGTH = 1048576; // 1 MB

    private int port;
    private AsyncChain<MetricsCollection, Boolean> processorChain;
    private TimeValue timeout;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private ScheduleContext context;
    private final Counter bufferedMetrics = Metrics.newCounter(HttpHandler.class, "Buffered Metrics");

    // Metrics
    private static final Timer handlerTimer = Metrics.newTimer(HttpHandler.class, "HTTP metrics ingestion timer",
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public HttpHandler(Integer portToListen, ScheduleContext context) {
        this(portToListen, context, null, DEFAULT_TIMEOUT);
    }

    public HttpHandler(Integer portToListen, ScheduleContext context, AsyncChain<List<Metric>, Boolean> processorChain, TimeValue timeout) {
        this.port = portToListen;
        this.timeout = timeout;
        this.context = context;

        if (processorChain == null) {
            this.processorChain = createDefaultProcessorChain();
        }

        RouteMatcher router = new RouteMatcher();
        router.get("/", new DefaultHandler());
        router.post("/metrics", new MetricsIngestor(this.processorChain, timeout));

        log.info("Starting metrics listener HTTP server on port {}", port);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(port));

    }

    private AsyncChain<MetricsCollection, Boolean> createDefaultProcessorChain() {
        return new AsyncChain<MetricsCollection, Boolean>()
                .withFunction(new TypeAndUnitProcessor(
                        new ThreadPoolBuilder().withName("Metric type and unit processing").build(),
                        metricMetadataAnalyzer)
                        .withLogger(log))
                .withFunction(new BatchSplitter(
                        new ThreadPoolBuilder().withName("Metric batching").build(),
                        WRITE_THREADS)
                        .withLogger(log))
                .withFunction(new BatchWriter(
                        new ThreadPoolBuilder()
                                .withName("Metric Batch Writing")
                                .withCorePoolSize(WRITE_THREADS)
                                .withMaxPoolSize(WRITE_THREADS)
                                .withUnboundedQueue()
                                .withRejectedHandler(new ThreadPoolExecutor.AbortPolicy())
                                .build(),
                        AstyanaxWriter.getInstance(),
                        timeout,
                        bufferedMetrics,
                        context)
                        .withLogger(log));
    }

    private class MetricsHttpServerPipelineFactory implements ChannelPipelineFactory {
        private RouteMatcher router;

        public MetricsHttpServerPipelineFactory(RouteMatcher router) {
            this.router = router;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            final ChannelPipeline pipeline = pipeline();

            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("encoder", new HttpResponseEncoder());
            pipeline.addLast("chunkaggregator", new HttpChunkAggregator(MAX_CONTENT_LENGTH));
            pipeline.addLast("handler", router);

            return pipeline;
        }
    }

    private class MetricsIngestor implements HttpRequestHandler {
        private final ObjectMapper mapper;
        private final TypeFactory typeFactory;
        private final AsyncChain<MetricsCollection, Boolean> processorChain;
        private final TimeValue timeout;

        public MetricsIngestor(AsyncChain<MetricsCollection, Boolean> processorChain, TimeValue timeout) {
            this.mapper = new ObjectMapper();
            this.typeFactory = TypeFactory.defaultInstance();
            this.timeout = timeout;
            this.processorChain = processorChain;
        }

        @Override
        public void handle(ChannelHandlerContext ctx, HttpRequest request) {
            JSONMetricsContainer jsonMetricsContainer = null;

            final TimerContext timerContext = handlerTimer.time();
            final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
            try {
                List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                        mapper.readValue(
                                body,
                                typeFactory.constructCollectionType(List.class,
                                        JSONMetricsContainer.JSONMetric.class)
                        );
                jsonMetricsContainer = new JSONMetricsContainer(jsonMetrics);
            } catch (JsonParseException e) {
                log.warn("Exception parsing content", e);
                sendResponse(ctx.getChannel(), "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (JsonMappingException e) {
                log.warn("Exception parsing content", e);
                sendResponse(ctx.getChannel(), "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (IOException e) {
                log.warn("IO Exception parsing content", e);
                sendResponse(ctx.getChannel(), "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
                return;
            } catch (Exception e) {
                log.warn("Other exception while trying to parse content", e);
                sendResponse(ctx.getChannel(), "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
                return;
            }

            if (jsonMetricsContainer == null) {
                sendResponse(ctx.getChannel(), null, HttpResponseStatus.OK);
                return;
            }

            List<Metric> metrics =  jsonMetricsContainer.toMetrics();
            if (metrics == null || metrics.isEmpty()) {
                sendResponse(ctx.getChannel(), null, HttpResponseStatus.OK);
                return;
            }

            final MetricsCollection collection = new MetricsCollection();
            collection.add(metrics);

            try {
                processorChain.apply(collection).get(timeout.getValue(), timeout.getUnit());
                sendResponse(ctx.getChannel(), null, HttpResponseStatus.OK);
            } catch (TimeoutException e) {
                sendResponse(ctx.getChannel(), "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
            } catch (Exception e) {
                log.error("Exception persisting metrics", e);
                sendResponse(ctx.getChannel(), "Error persisting metrics", HttpResponseStatus.INTERNAL_SERVER_ERROR);
            } finally {
                timerContext.stop();
            }
        }

        public void sendResponse(Channel channel, String messageBody, HttpResponseStatus status) {
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

            if (messageBody != null && !messageBody.isEmpty()) {
                response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
            }
            channel.write(response);
        }
    }
}
