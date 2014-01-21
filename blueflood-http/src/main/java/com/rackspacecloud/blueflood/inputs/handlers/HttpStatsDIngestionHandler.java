/*
 * Copyright 2013 Rackspace
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

import com.codahale.metrics.Timer;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.Bundle;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class HttpStatsDIngestionHandler implements HttpRequestHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(HttpStatsDIngestionHandler.class);
    
    private static final Timer handlerTimer = Metrics.timer(HttpStatsDIngestionHandler.class, "HTTP statsd metrics ingestion timer");
    
    private AstyanaxWriter writer;
    
    public HttpStatsDIngestionHandler() {
        writer = AstyanaxWriter.getInstance();
    }
    
    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        
        final Timer.Context timerContext = handlerTimer.time();
        
        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);

        Bundle bundle;
        try {
            bundle = createBundle(body);
        } catch (JsonParseException ex) {
            logger.error("BAD JSON: %s", body);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "Bad JSON", HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        // we want this to block until things get ingested.
        
        // convert, then write, then respond.
        Collection<IMetric> metrics = PreaggregateConversions.buildMetricsCollection(bundle);
        
        try {
            writer.insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
        } catch (ConnectionException ex) {
            logger.error(ex.getMessage(), ex);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "Internal error saving data", HttpResponseStatus.INTERNAL_SERVER_ERROR); 
        } finally {
            timerContext.stop();
        }
    }
    
    public static Bundle createBundle(String json) {
        Bundle bundle = new Gson().fromJson(json, Bundle.class);
        return bundle;
    }
}
