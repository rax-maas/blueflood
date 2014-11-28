/*
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.Bundle;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

public class HttpStatsDIngestionHandler implements HttpRequestHandler {
    
    private static final Logger log = LoggerFactory.getLogger(HttpStatsDIngestionHandler.class);
    
    private static final Timer handlerTimer = Metrics.timer(HttpStatsDIngestionHandler.class, "HTTP statsd metrics ingestion timer");
    private static final Counter requestCount = Metrics.counter(HttpStatsDIngestionHandler.class, "HTTP Request Count");

    private AsyncChain<String, List<Boolean>> processorChain;
    private final TimeValue timeout;
    
    public HttpStatsDIngestionHandler(AsyncChain<String,List<Boolean>> processorChain, TimeValue timeout) {
        this.processorChain = processorChain;
        this.timeout = timeout;
    }
    
    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        
        final Timer.Context timerContext = handlerTimer.time();
        
        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
	    // block until things get ingested.
            requestCount.inc();
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
            requestCount.dec();
            timerContext.stop();
        }
    }
    
    public static Bundle createBundle(String json) {
        Bundle bundle = new Gson().fromJson(json, Bundle.class);
        return bundle;
    }
    
    public static class MakeBundle implements AsyncFunction<String, Bundle> {
        @Override
        public ListenableFuture<Bundle> apply(String input) throws Exception {
            return new NoOpFuture<Bundle>(createBundle(input));
        }
    }
    
    public static class MakeCollection implements AsyncFunction<Bundle, MetricsCollection> {
        @Override
        public ListenableFuture<MetricsCollection> apply(Bundle input) throws Exception {
            MetricsCollection collection = new MetricsCollection();
            collection.add(PreaggregateConversions.buildMetricsCollection(input));
            return new NoOpFuture<MetricsCollection>(collection);
        }
    }
    
    public static class WriteMetrics extends AsyncFunctionWithThreadPool<Collection<IMetric>, Boolean> {
        private final AstyanaxWriter writer;
        
        public WriteMetrics(ThreadPoolExecutor executor, AstyanaxWriter writer) {
            super(executor);
            this.writer = writer;
        }

        @Override
        public ListenableFuture<Boolean> apply(final Collection<IMetric> input) throws Exception {
            return this.getThreadPool().submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    writer.insertMetrics(input, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
                    return true;
                }
            });
        }
    }
}
