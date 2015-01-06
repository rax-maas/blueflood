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
package com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.gson.Gson;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.download.CloudFilesManager;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.exceptions.OutOFBandException;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.gson.CheckFromJson;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.gson.MetricPoint;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.service.BackFillerConfig;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.service.OutOFBandRollup;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class BuildStore {
    private static final BuildStore buildStore = new BuildStore();
    private static final Logger log = LoggerFactory.getLogger(BuildStore.class);
    // number of metrics coming in for ranges which we have already rolled up that can be tolerated
    private static final int OUT_OF_RANGE_TOLERATION_THRESHOLD = 10;
    // please getEligibleData for better understanding of what is this parameter
    private static final int RANGE_BUFFER = Configuration.getInstance().getIntegerProperty(BackFillerConfig.NUMBER_OF_BUFFERED_SLOTS);
    private static int outOfRangeToleration = 0;
    // Specifies the sorting order in TreeMap
    private static Comparator<Range> rangeComparator = new Comparator<Range>() {
        @Override public int compare(Range r1, Range r2) {
            return (int) (r1.getStart() - r2.getStart());
        }
    };
    /*
     * This is a concurrent version of TreeMap.
     * Reason behind using a TreeMap : We want to keep the mapping from ranges to metrics sorted
     * Reason for using concurrent version : We do not want to block on simultaneous read(happens in RollupGenerator), write(happens here). Both these operations should not overlap
     * due to the range buffer
     */
    private static ConcurrentSkipListMap<Range, ConcurrentHashMap<Locator, Points>> locatorToTimestampToPoint = new ConcurrentSkipListMap<Range, ConcurrentHashMap<Locator, Points>>(rangeComparator);
    // Fixed list of ranges within the replay period to rollup
    private static List<Range> rangesToRollup = new ArrayList<Range>();
    //Only one thread will be calling this. No need to make this thread safe. Shrinking subset of ranges within replay period
    public static List<Range> rangesStillApplicable = new LinkedList<Range>();
    private static Meter completedRangesReturned = Metrics.meter(BuildStore.class, "Number of Ranges filled up meter");
    private static Meter metricsParsedAndMergedMeter = Metrics.meter(BuildStore.class, "Number of metrics parsed per unit time");
    private static Counter invalidMetricsCounter = Metrics.counter(BuildStore.class, "Invalid metrics found while parsing");
    private static Meter metricCannotBeParsed = Metrics.meter(BuildStore.class, "Unable to parse metrics");
    private static final Collection<Integer> shardsToBackfill = Collections.unmodifiableCollection(
            Util.parseShards(Configuration.getInstance().getStringProperty(BackFillerConfig.SHARDS)));

    static {
        for(Range range : CloudFilesManager.ranges) {
            rangesToRollup.add(range);
            rangesStillApplicable.add(range);
        }
        log.info("Added the first range as "+rangesToRollup.get(0)+" last range as "+rangesToRollup.get(rangesToRollup.size()-1));
    }

    public static BuildStore getBuilder() {
        return buildStore;
    }

    public static void merge (InputStream jsonInput) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(jsonInput));
        Gson gson = new Gson();
        String line = reader.readLine();
        try {
            while (line != null) {
                line = line.trim();
                if (line.length() == 0) {
                    line = reader.readLine();
                    continue;
                }

                CheckFromJson checkFromJson = gson.fromJson(line, CheckFromJson.class);

                long timestamp = checkFromJson.getTimestamp();
                long snappedMillis = Granularity.MIN_5.snapMillis(timestamp);

                Range rangeOfThisTimestamp = new Range(snappedMillis, snappedMillis + Granularity.MIN_5.milliseconds() - 1);

                // Do not add timestamps that lie out of range
                if (!rangesToRollup.contains(rangeOfThisTimestamp)) {
                    log.warn("Timestamp of metrics of check found lying out the range: "+rangeOfThisTimestamp+" TS: "+timestamp);
                    line = reader.readLine();
                    continue;
                }

                //These are out of band timestamps lying in the ranges which we have already rolled
                if (rangesToRollup.contains(rangeOfThisTimestamp) && !rangesStillApplicable.contains(rangeOfThisTimestamp)) {
                    log.warn("Range of timestamp of metrics of check " + checkFromJson.getCheckId() + "is out of applicable ranges");
                    outOfRangeToleration++;

                    // If we are seeing a lot of out of band metrics, something is wrong. May be metrics are back logged a lot. stop immediately. try to increase the range buffer?
                    if (outOfRangeToleration > OUT_OF_RANGE_TOLERATION_THRESHOLD) {
                        throw new OutOFBandException("Starting to see a lot of metrics in non-applicable ranges");
                    }
                    line = reader.readLine();
                    continue;
                }

                if (!CheckFromJson.isValid(checkFromJson)) {
                    invalidMetricsCounter.inc();
                } else {
                    for (String metricName : checkFromJson.getMetricNames()) {
                        MetricPoint metricPoint;

                        try {
                            metricPoint = checkFromJson.getMetric(metricName);
                        } catch (RuntimeException e) {
                            //pass. Let the user determine if he wants to continue by just emitting a metric
                            metricCannotBeParsed.mark();
                            continue;
                        }

                        // Bail out for string metrics
                        if (metricPoint.getType() == String.class) continue;

                        Locator metricLocator;
                        if (checkFromJson.getCheckType().contains("remote")) {
                            String longMetricName = String.format("%s.rackspace.monitoring.entities.%s.checks.%s.%s.%s.%s",
                                    checkFromJson.getTenantId(),
                                    checkFromJson.getEntityId(),
                                    checkFromJson.getCheckType(),
                                    checkFromJson.getCheckId(),
                                    checkFromJson.getMonitoringZoneId(),
                                    metricName).trim();
                            metricLocator = Locator.createLocatorFromDbKey(longMetricName);
                        } else {
                            String longMetricName = String.format("%s.rackspace.monitoring.entities.%s.checks.%s.%s.%s",
                                    checkFromJson.getTenantId(),
                                    checkFromJson.getEntityId(),
                                    checkFromJson.getCheckType(),
                                    checkFromJson.getCheckId(),
                                    metricName).trim();
                            metricLocator = Locator.createLocatorFromDbKey(longMetricName);
                        }

                        if (!shardsToBackfill.contains(Util.computeShard(metricLocator.toString()))) continue;

                        // The following it required because concurrent data structure provides weak consistency. For eg. Two threads both calling get will see different results. putIfAbsent provides atomic operation
                        ConcurrentHashMap<Locator, Points> tsToPoint = locatorToTimestampToPoint.get(rangeOfThisTimestamp);
                        if(tsToPoint == null) {
                            // Need to synchronize this HashMap, because multiple threads might be calling containsKey and then putting values at the same time
                            final ConcurrentHashMap<Locator, Points> tsToPointVal = new ConcurrentHashMap<Locator, Points>();
                            tsToPoint = locatorToTimestampToPoint.putIfAbsent(rangeOfThisTimestamp, tsToPointVal);
                            if (tsToPoint == null) {
                               tsToPoint = tsToPointVal;
                            }
                        }

                        Points points = tsToPoint.get(metricLocator);
                        if (points == null) {
                            Points pointsToPut = new Points();
                            points = tsToPoint.putIfAbsent(metricLocator, pointsToPut);
                            if (points == null) {
                                points = pointsToPut;
                            }
                        }

                        points.add(new Points.Point(timestamp, new SimpleNumber(metricPoint.getValue())));
                        metricsParsedAndMergedMeter.mark();
                    }
                }

                line = reader.readLine();
            }
        } catch (OutOFBandException e) {
            RollupGenerator.rollupExecutors.shutdownNow();
            OutOFBandRollup.getRollupGeneratorThread().interrupt();
            // Stop the monitoring thread
            OutOFBandRollup.getMonitoringThread().interrupt();
            // Stop the file handler thread pool from sending data to buildstore
            FileHandler.handlerThreadPool.shutdownNow();
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception encountered while merging file into build store", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        locatorToTimestampToPoint.clear();
    }

    /*
     * Consider 5 files getting merged in the buildstore. Assuming the flush rate to be 1 file/min, we need all the files together in order to construct 5 min rollup
     * How are we going to decide that a particular range has been totally filled up and ready to be rolled?
     * One behaviour which we will start seeing in the buildstore is "higher" ranges starting to build up. This means the current range has almost filled up.
     * But there is still a possibility for backed up data, getting merged. So we provide RANGE_BUFFER.
     * Basically, for every call to getEligibleData, it is going to return (n-RANGE_BUFFER) ranges to get rolled up, and keep (RANGE_BUFFER) in buildstore
     * Also, note that returning the range, will eventually remove them from buildstore, after all rollups are completed in RollupGenerator for that range.
     */
    public static Map<Range, ConcurrentHashMap<Locator, Points>> getEligibleData() {
        if (locatorToTimestampToPoint.size() <= RANGE_BUFFER) {
            log.debug("Range buffer still not exceeded. Returning null data to rollup generator");
            return null;
        } else {
            Object[] sortedKeySet = locatorToTimestampToPoint.keySet().toArray();
            Range cuttingPoint = (Range) sortedKeySet[sortedKeySet.length - RANGE_BUFFER - 1];
            log.info("Found completed ranges up to the threshold range of {}", cuttingPoint);
            completedRangesReturned.mark();
            return locatorToTimestampToPoint.headMap(cuttingPoint, true);
        }
    }
}
