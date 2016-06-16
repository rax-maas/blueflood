/*
 * Copyright 2016 Rackspace
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

package com.rackspacecloud.blueflood.outputs.handlers;

import com.codahale.metrics.Timer;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.io.LocatorIO;
import com.rackspacecloud.blueflood.tracker.Tracker;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class HttpShardedMetricsHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpShardedMetricsHandler.class);
    private final LocatorIO io = IOContainer.fromConfig().getLocatorIO();
    private final Timer httpShardedMetricsFetchTimer = Metrics.timer(HttpShardedMetricsHandler.class,
            "Handle HTTP request for metrics by shard");

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        Tracker.getInstance().track(request);
        final String tenantId = request.getHeader("tenantId");
        final String shardId = request.getHeader("shard");
        final Timer.Context httpShardedMetricsFetchTimerContext = httpShardedMetricsFetchTimer.time();

        try {
            int shard = Integer.parseInt(shardId);
            if (shard < 0 || shard >= Constants.NUMBER_OF_SHARDS) {
                throw new InvalidDataException("Invalid shard number in query.");
            }
            Collection<Locator> sharded = io.getLocators(shard);
            Collection<Locator> filtered = Collections2.filter(sharded, new Predicate<Locator>() {
                @Override
                public boolean apply(Locator input) {
                    return Objects.equals(input.getTenantId(), tenantId);
                }
            });
            sendResponse(ctx, request, getSerializedJSON(filtered), HttpResponseStatus.OK);
        } catch (NumberFormatException e) {
            log.warn(e.getMessage());
            sendResponse(ctx, request, "Invalid shard", HttpResponseStatus.BAD_REQUEST);
        } catch (InvalidDataException e) {
            log.warn(e.getMessage());
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpShardedMetricsFetchTimerContext.stop();
        }
    }

    private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }

    public static String getSerializedJSON(Iterable<Locator> locators) {
        ArrayNode resultArray = JsonNodeFactory.instance.arrayNode();
        for (Locator locator: locators) {
            resultArray.add(locator.getMetricName());
        }
        return resultArray.toString();
    }
}
