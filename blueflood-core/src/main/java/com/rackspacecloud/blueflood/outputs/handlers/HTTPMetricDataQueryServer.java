package com.rackspacecloud.blueflood.outputs.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.rackspacecloud.blueflood.exceptions.InvalidRequestException;
import com.rackspacecloud.blueflood.exceptions.SerializationException;
import com.rackspacecloud.blueflood.http.*;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.outputs.serializers.JSONOutputSerializer;
import com.rackspacecloud.blueflood.outputs.serializers.OutputSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Resolution;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.channel.Channels.pipeline;

public class HTTPMetricDataQueryServer {
    private static final Logger log = LoggerFactory.getLogger(HTTPMetricDataQueryServer.class);
    private final int httpQueryPort;
    private AstyanaxReader reader = AstyanaxReader.getInstance();

    public HTTPMetricDataQueryServer(Integer portToListen) {
        this.httpQueryPort = portToListen;

        RouteMatcher router = new RouteMatcher();
        router.get("/v1.0", new DefaultHandler());
        router.get("/v1.0/:accountId/experimental/views/metric_data/:metricName", new HTTPRollupsQueryHandler());

        log.info("Starting metrics listener HTTP server on port {}", this.httpQueryPort);
        ServerBootstrap server = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        server.setPipelineFactory(new MetricsHttpServerPipelineFactory(router));
        server.bind(new InetSocketAddress(httpQueryPort));
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
            pipeline.addLast("handler", new QueryStringDecoderAndRouter(router));

            return pipeline;
        }

    }

    static class HTTPRollupsQueryHandler extends RollupHandler
            implements MetricDataQueryInterface<JSONObject>, HttpRequestHandler {
        private final OutputSerializer<JSONObject> serializer;
        private final Gson gson;           // thread-safe
        private final JsonParser parser;   // thread-safe
        private final Set<String> defaultStats;

        private final Timer httpMetricsFetchTimer = Metrics.newTimer(HTTPRollupsQueryHandler.class,
                "Handle HTTP request for metrics", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        public HTTPRollupsQueryHandler() {
            this.serializer = new JSONOutputSerializer();
            gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
            parser = new JsonParser();
            defaultStats = new HashSet<String>();
            defaultStats.add("average");
        }

        @Override
        public JSONObject GetDataByPoints(String accountId,
                                          String metric,
                                          long from,
                                          long to,
                                          int points) throws SerializationException {
            rollupsByPointsMeter.mark();
            Granularity g = Granularity.granularityFromPointsInInterval(from, to, points);
            MetricData data = getRollupByGranularity(accountId, metric, from, to, g);
            return serializer.transformRollupData(data, defaultStats);
        }

        @Override
        public JSONObject GetDataByResolution(String accountId,
                                              String metric,
                                              long from,
                                              long to,
                                              Resolution resolution) throws SerializationException {
            rollupsByGranularityMeter.mark();
            if (resolution == null) {
                resolution = Resolution.FULL;
            }
            Granularity g = Granularity.granularities()[resolution.getValue()];
            MetricData data = getRollupByGranularity(accountId, metric, from, to, g);
            return serializer.transformRollupData(data, defaultStats);
        }

        @Override
        public void handle(ChannelHandlerContext ctx, HttpRequest request) {
            final String accountId = request.getHeader("accountId");
            final String metricName = request.getHeader("metricName");

            if (!(request instanceof HTTPRequestWithDecodedQueryParams)) {
                sendResponse(ctx, request, "Missing query params: from, to, points",
                        HttpResponseStatus.BAD_REQUEST);
                return;
            }

            HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

            final TimerContext httpMetricsFetchTimerContext = httpMetricsFetchTimer.time();
            try {
                RollupsQueryParams params = parseParams(requestWithParams.getQueryParams());

                JSONObject metricData;
                if (params.isPoints) {
                    metricData = GetDataByPoints(accountId, metricName, params.from, params.to, params.points);
                } else if (params.isResolution) {
                    metricData = GetDataByResolution(accountId, metricName, params.from, params.to, params.resolution);
                } else {
                    throw new InvalidRequestException("Invalid rollups query. Neither points nor resolution specified.");
                }

                final JsonElement element = parser.parse(metricData.toString());
                final String jsonStringRep = gson.toJson(element);
                sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
            } catch (InvalidRequestException e) {
                sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
            } catch (SerializationException e) {
                sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
            } catch (Exception e) {
                sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
            } finally {
                httpMetricsFetchTimerContext.stop();
            }
        }

        public RollupsQueryParams parseParams(Map<String, List<String>> params) throws InvalidRequestException {
            if (params == null || params.isEmpty()) {
                throw new InvalidRequestException("No query parameters present.");
            }

            List<String> points = params.get("points");
            List<String> res = params.get("resolution");
            List<String> from = params.get("from");
            List<String> to = params.get("to");

            if (points == null && res == null) {
                throw new InvalidRequestException("Either 'points' or 'resolution' is required.");
            }

            if (points != null && (points.isEmpty() || points.size() > 1)) {
                throw new InvalidRequestException("Invalid parameter: points=" + points);
            } else if (res != null && (res.isEmpty() || res.size() > 1)) {
                throw new InvalidRequestException("Invalid parameter: resolution=" + res);
            } else if (from == null || from.size() > 1) {
                throw new InvalidRequestException("Invalid parameter: from=" + from);
            } else if (to == null || to.size() > 1) {
                throw new InvalidRequestException("Invalid parameter: to="+ to);
            }

            long fromTime = Long.parseLong(from.get(0));
            long toTime = Long.parseLong(to.get(0));

            if (toTime < fromTime) {
                throw new InvalidRequestException("paramter 'to' must be greater than 'from'");
            }

            if (points != null) {
                return new RollupsQueryParams(fromTime, toTime, Integer.parseInt(points.get(0)));
            } else {
                return new RollupsQueryParams(fromTime, toTime, ResolutionMapper.getByName(res.get(0)));
            }
        }

        public void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                                 HttpResponseStatus status) {
            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

            if (messageBody != null && !messageBody.isEmpty()) {
                response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
            }
            HttpResponder.respond(channel, request, response);
        }

        private class RollupsQueryParams {
            public int points;
            public Resolution resolution;
            public long from;
            public long to;
            public boolean isPoints = true;
            public boolean isResolution = false;

            public RollupsQueryParams(long from, long to, int points) {
                this.from = from;
                this.to = to;
                this.points = points;
                this.isResolution = false;
                this.isPoints = true;
            }

            public RollupsQueryParams(long from, long to, Resolution resolution) {
                this.from = from;
                this.to = to;
                this.resolution = resolution;
                this.isPoints = false;
                this.isResolution = true;
            }
        }
    }

    public static class ResolutionMapper {
        private static Map<String, Resolution> resolutionMap = new HashMap<String, Resolution>();

        static {
            resolutionMap.put("FULL", Resolution.FULL);

            resolutionMap.put("5MIN", Resolution.MIN5);
            resolutionMap.put("MIN5", Resolution.MIN5);

            resolutionMap.put("20MIN", Resolution.MIN20);
            resolutionMap.put("MIN20", Resolution.MIN20);

            resolutionMap.put("60MIN", Resolution.MIN60);
            resolutionMap.put("MIN60", Resolution.MIN60);

            resolutionMap.put("240MIN", Resolution.MIN240);
            resolutionMap.put("MIN240", Resolution.MIN240);

            resolutionMap.put("1440MIN", Resolution.MIN1440);
            resolutionMap.put("MIN1440", Resolution.MIN1440);
        }

        public static Resolution getByName(String name) {
            return resolutionMap.get(name.toUpperCase());
        }
    }
}
