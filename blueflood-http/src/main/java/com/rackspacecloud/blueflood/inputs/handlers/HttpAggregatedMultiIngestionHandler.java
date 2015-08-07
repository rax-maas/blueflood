package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.*;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.Bundle;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Created by tilo on 8/7/15.
 */
public class HttpAggregatedMultiIngestionHandler implements HttpRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpAggregatedIngestionHandler.class);

    private static final Timer handlerTimer = Metrics.timer(HttpAggregatedIngestionHandler.class, "HTTP statsd metrics ingestion timer");
    private static final Counter requestCount = Metrics.counter(HttpAggregatedIngestionHandler.class, "HTTP Request Count");

    private final HttpMetricsIngestionServer.Processor processor;
    private final TimeValue timeout;

    public HttpAggregatedMultiIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        this.processor = processor;
        this.timeout = timeout;
    }

    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {

        Tracker.track(request);

        final Timer.Context timerContext = handlerTimer.time();

        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
            // block until things get ingested.
            requestCount.inc();
            List<Bundle> bundleList = createBundleList(body);
            for (Bundle bundle : bundleList){
                MetricsCollection collection = new MetricsCollection();
                collection.add(PreaggregateConversions.buildMetricsCollection(bundle));
                ListenableFuture<List<Boolean>> futures = processor.apply(collection);
                List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
                for (Boolean persisted : persisteds) {
                    if (!persisted) {
                        DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                        return;
                    }
                }
            }
            DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);

        } catch (JsonParseException ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (ConnectionException ex) {
            log.error(ex.getMessage(), ex);
            DefaultHandler.sendResponse(ctx, request, "Internal error saving data", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException ex) {
            DefaultHandler.sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception ex) {
            log.debug(String.format("BAD JSON: %s", body));
            log.error("Other exception while trying to parse content", ex);
            DefaultHandler.sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            requestCount.dec();
            timerContext.stop();
        }
    }

    public static List<Bundle> createBundleList(String json) {
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonArray jArray = parser.parse(json).getAsJsonArray();

        ArrayList<Bundle> bundleList = new ArrayList<Bundle>();

        for(JsonElement obj : jArray )
        {
            Bundle bundle = gson.fromJson( obj , Bundle.class);
            bundleList.add(bundle);
        }

        return bundleList;
    }
}
