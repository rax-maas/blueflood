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

import com.google.gson.Gson;
import com.google.gson.internal.LazilyParsedNumber;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.inputs.handlers.wrappers.Bundle;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.PreaggregatedMetric;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpStatsDIngestionHandler implements HttpRequestHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(HttpStatsDIngestionHandler.class);
    private static final String NAME_DELIMITER = "//.";
    
    // todo: this needs to be set some other way. punting for now.
    private static final TimeValue DEFAULT_TTL = new TimeValue(48, TimeUnit.HOURS);
    
    private AstyanaxWriter writer;
    
    public HttpStatsDIngestionHandler() {
        writer = AstyanaxWriter.getInstance();
    }
    
    // our own stuff.
    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        // ok. let's see whats in the headers, etc. I need to find out what's been put in, etc.
        logger.info("HEADERS...");
        for (Map.Entry header : request.getHeaders()) {
            logger.info(String.format("%s = %s", header.getKey().toString(), header.getValue().toString()));
        }
        
        // this is all JSON.
        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);

        // todo: what happens when crappy JSON gets sent in?
        
        Bundle bundle = createBundle(body);
        logger.info(String.format("BUNDLE: %s", bundle.toString()));
        
        // we want this to block until things get ingested.
        
        // convert, then respond.
        Collection<IMetric> metrics = buildMetrics(bundle);
        
        try {
            writer.insertMetrics(metrics, CassandraModel.CF_METRICS_PREAGGREGATED_FULL);
            HttpMetricsIngestionHandler.sendResponse(ctx, request, null, HttpResponseStatus.OK);
        } catch (ConnectionException ex) {
            HttpMetricsIngestionHandler.sendResponse(ctx, request, "", HttpResponseStatus.INTERNAL_SERVER_ERROR); 
        }
    }
    
    public static Bundle createBundle(String json) {
        Bundle bundle = new Gson().fromJson(json, Bundle.class);
        return bundle;
    }
    
    public static Collection<IMetric> buildMetrics(Bundle bundle) {
        Collection<IMetric> metrics = new ArrayList<IMetric>();
        metrics.addAll(convertCounters(bundle.getTenantId(), bundle.getTimestamp(), bundle.getCounters()));
        metrics.addAll(convertGauges(bundle.getTenantId(), bundle.getTimestamp(), bundle.getGauges()));
        metrics.addAll(convertSets(bundle.getTenantId(), bundle.getTimestamp(), bundle.getSets()));
        metrics.addAll(convertTimers(bundle.getTenantId(), bundle.getTimestamp(), bundle.getTimers()));
        return metrics;
    }

    public static Collection<PreaggregatedMetric> convertCounters(String tenant, long timestamp, Collection<Bundle.Counter> counters) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(counters.size());
        for (Bundle.Counter counter : counters) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, counter.getName().split(NAME_DELIMITER, -1));
            Rollup rollup = new CounterRollup()
                    .withCount(counter.getValue())
                    .withRate(counter.getRate().doubleValue())
                    // todo: if we knew the flush period, we could estimate the sample count.  consider sending that as
                    // part of the bundle.
                    .withSampleCount(1);
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
    public static Collection<PreaggregatedMetric> convertGauges(String tenant, long timestamp, Collection<Bundle.Gauge> gauges) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(gauges.size());
        for (Bundle.Gauge gauge : gauges) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, gauge.getName().split(NAME_DELIMITER, -1));
            Points<SimpleNumber> points = new Points<SimpleNumber>();
            points.add(new Points.Point<SimpleNumber>(timestamp, new SimpleNumber(resolveNumber(gauge.getValue()))));
            try {
                Rollup rollup = GaugeRollup.buildFromRawSamples(points);
                PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
                list.add(metric);
            } catch (IOException ex) {
                throw new IOError(ex);
            }
            
                    
        }
        return list;
    }
    
    public static Collection<PreaggregatedMetric> convertTimers(String tenant, long timestamp, Collection<Bundle.Timer> timers) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(timers.size());
        for (Bundle.Timer timer : timers) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, timer.getName().split(NAME_DELIMITER, -1));
            TimerRollup rollup = new TimerRollup()
                    .withCount(timer.getCount().longValue())
                    .withSampleCount(1)
                    .withAverage(resolveNumber(timer.getAvg()))
                    .withMaxValue(resolveNumber(timer.getMax()))
                    .withMinValue(resolveNumber(timer.getMin()))
                    .withCountPS(timer.getRate().doubleValue())
                    .withSum(timer.getSum().longValue()) // I wonder now if assuming sum instanceof Long is wrong.
                    .withVariance(Math.pow(timer.getStd().doubleValue(), 2d));
            for (Map.Entry<String, Bundle.Percentile> entry : timer.getPercentiles().entrySet()) {
                // throw away max and sum.
                rollup.setPercentile(entry.getKey(), entry.getValue().getAvg());
            }
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
//    meh. set's not supported until they are refactored.
    public static Collection<PreaggregatedMetric> convertSets(String tenant, long timestamp, Collection<Bundle.Set> sets) {
        List<PreaggregatedMetric> list = new ArrayList<PreaggregatedMetric>(sets.size());
        for (Bundle.Set set : sets) {
            Locator locator = Locator.createLocatorFromPathComponents(tenant, set.getName().split(NAME_DELIMITER, -1));
            SetRollup rollup = new SetRollup();
            for (String value : set.getValues()) {
                // todo: need to support this kind of set rollup.
//                rollup.merge(value.hashCode());
            }
            // for now, we'll do this
            rollup.setCount(set.getValues().length);
            PreaggregatedMetric metric = new PreaggregatedMetric(timestamp, locator, DEFAULT_TTL, rollup);
            list.add(metric);
        }
        return list;
    }
    
    // resolve a number to a Long or double.
    public static Number resolveNumber(Number n) {
        if (n instanceof LazilyParsedNumber) {
            try {
                return n.longValue();
            } catch (NumberFormatException ex) {
                return n.doubleValue();
            }
        } else {
            // already resolved.
            return n;
        }
    }
    
    
}
