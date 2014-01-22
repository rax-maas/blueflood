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

package com.rackspacecloud.blueflood.tools.ops;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.exceptions.GranularityException;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RollupTool {
    private static final Options cliOptions = new Options();
    private static final GnuParser parser = new GnuParser();
    private static final HelpFormatter helpFormatter = new HelpFormatter();
    private static final String TENANT_ID = "tenantId";
    private static final String METRIC = "metric";
    private static final String FROM = "from";
    private static final String TO = "to";
    //Number of threads that will be updating the cache simultaneously.
    //Has been set to 1, for standalone use. If someone wraps these around some threadpool
    //set this to number of threads that will be using this class.
    private static final int METADATA_CACHE_CONCURRENCY = 1;
    private static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS),
            METADATA_CACHE_CONCURRENCY);

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Tenant ID").create(TENANT_ID));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Metric name").create(METRIC));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));
    }

    public static void main(String args[]) {
        Map<String, Object> options = parseOptions(args);
        Locator locator = Locator.createLocatorFromPathComponents(
                (String) options.get(TENANT_ID),
                (String) options.get(METRIC));
        Long from = (Long) options.get(FROM);
        Long to = (Long) options.get(TO);

        if (from >= to) {
            System.err.println("End time " + to + " has to be greater than start time " + from);
            System.exit(2);
        }

        rerollData(locator, new Range(from, to));
    }

    private static Map<String, Object> parseOptions(String[] args) {
        CommandLine line;
        final Map<String, Object> options = new HashMap<String, Object>();

        try {
            line = parser.parse(cliOptions, args);

            if (line.hasOption(TENANT_ID)) {
                options.put(TENANT_ID, line.getOptionValue(TENANT_ID));
            }

            if (line.hasOption(METRIC)) {
                options.put(METRIC, line.getOptionValue(METRIC));
            }

            if (line.hasOption(FROM)) {
                options.put(FROM, new Long(line.getOptionValue(FROM)));
            }

            if (line.hasOption(TO)) {
                options.put(TO, new Long(line.getOptionValue(TO)));
            }

        } catch (ParseException ex) {
            System.err.println("Parse exception " + ex.getMessage());
            helpFormatter.printHelp("ReRoll Data", cliOptions);
            System.exit(2);
        }
        return options;
    }

    public static void rerollData(Locator loc, Range range) {
        RollupType rollupType = null;

        try {
            rollupType = RollupType.fromString(rollupTypeCache.get(
                    loc, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));
        } catch (CacheException e) {
            System.err.println("Exception encountered while grabbing metadata for the locator for cache "+e.getMessage());
            System.exit(-1);
        }

        if(rollupType == null) {
            System.err.println("Rollup Type encountered for metric was null");
            System.exit(-1);
        }

        Granularity[] rollupGrans = Granularity.rollupGranularities();
        for (Granularity gran : rollupGrans) {
            rerollDataPerGran(loc, gran, range, rollupType);
        }
    }

    private static void rerollDataPerGran(Locator loc, Granularity gran, Range range, RollupType rollupType) {
        try {
            //Get the source and destination column families
            Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, gran);
            ColumnFamily<Locator, Long> srcCF = CassandraModel.getColumnFamily(rollupClass, gran.finer());
            ColumnFamily<Locator, Long> dstCF = CassandraModel.getColumnFamily(rollupClass, gran);
            System.out.println("Calculating rollups for " + gran.name() + ". Reading from: " + srcCF.getName() + ". Writing to: " + dstCF.getName());
            //Get Rollup Computer
            Rollup.Type rollupComputer = RollupRunnable.getRollupComputer(rollupType, gran.finer());
            //This needs some explanation: Here we take slots in the gran to which we are rolling up i.e dstgran
            //Then for each slot we grab the data from the srcCF, thus automatically grabbing the sub-slots in the finer gran
            //Also, we always use the supplied time range to find snapped times, because a rollup gran is always a multiple
            //of the grans that come before it.
            Iterable<Range> ranges = Range.rangesForInterval(gran, range.getStart(), range.getStop());
            int count = 1;
            ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
            for (Range r : ranges) {
                Points input;
                Rollup rollup = null;
                try {
                    input = AstyanaxReader.getInstance().getDataToRoll(rollupClass,
                            loc, r, srcCF);
                    rollup = rollupComputer.compute(input);
                } catch (IOException ex) {
                    System.err.println("IOException while getting points to roll " + ex.getMessage());
                    System.exit(-1);
                }
                writeContexts.add(new SingleRollupWriteContext(rollup, new SingleRollupReadContext(loc, r, gran), dstCF));
                count++;
            }
            //Insert calculated Rollups back into destination CF
            try {
                AstyanaxWriter.getInstance().insertRollups(writeContexts);
            } catch (ConnectionException ex) {
                System.err.println("Connection exception while inserting rollups" + ex.getMessage());
                System.exit(-1);
            }
            System.out.println("Rolled up " + count + " ranges of " + gran+"\n");
        } catch (GranularityException e) {
            // Since we start rolling from 5m, we should never reach here.
            System.err.println("Unexpected exception encountered " + e.getMessage());
            System.exit(-1);
        }
    }
}
