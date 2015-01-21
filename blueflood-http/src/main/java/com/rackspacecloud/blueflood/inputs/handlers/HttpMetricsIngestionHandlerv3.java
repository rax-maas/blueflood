package com.rackspacecloud.blueflood.inputs.handlers;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.MetricsBundle;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;

public class HttpMetricsIngestionHandlerv3 implements HttpRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionHandlerv3.class);

    private AsyncChain<String, List<Boolean>> processorChain;
    private final TimeValue timeout;

    public HttpMetricsIngestionHandlerv3(AsyncChain<String, List<Boolean>> v3ProcessorChain, TimeValue timeout) {
        this.processorChain = v3ProcessorChain;
        this.timeout = timeout;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
            // block until things get ingested.
            ListenableFuture<List<Boolean>> futures = processorChain.apply(body);
            List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
            for (Boolean persisted : persisteds) {
                if (!persisted) {
                    HttpMetricsIngestionHandler.sendResponse(ctx, request, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    return;
                }
            }
            HttpMetricsIngestionHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);

        } catch (JsonParseException ex) {
            log.error("BAD JSON: %s", body);
            log.error(ex.getMessage(), ex);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (ConnectionException ex) {
            log.error(ex.getMessage(), ex);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "Internal error saving data", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException ex) {
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception ex) {
            log.warn("Other exception while trying to parse content", ex);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            // requestCount.dec();
            // timerContext.stop();
        }

    }

    public static MetricsBundle createBundle(String json) {
        MetricsBundle bundle = new Gson().fromJson(json, MetricsBundle.class);
        return bundle;
    }

    public static class MakeBundle implements AsyncFunction<String, MetricsBundle> {
        @Override
        public ListenableFuture<MetricsBundle> apply(String input) throws Exception {
            return new NoOpFuture<MetricsBundle>(createBundle(input));
        }
    }

    public static class MakeCollection implements AsyncFunction<MetricsBundle, MetricsCollection> {
        @Override
        public ListenableFuture<MetricsCollection> apply(MetricsBundle input) throws Exception {
            MetricsCollection collection = new MetricsCollection();
            // TODO: change this for MetricsBundle
            collection.add(PreaggregateConversions.buildMetricsCollection(null));
            return new NoOpFuture<MetricsCollection>(collection);
        }
    }
}
