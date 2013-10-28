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

package com.rackspacecloud.blueflood.tools.ops;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;

import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Usage:
 *
 * $JAVA -cp $CLASSPATH GetPoints -tenantId ${tenantId} -metric ${metName} -from ${startTime} \
 *  -to ${endTime} -resolution ${res}
 *
 * tenantId - Tenant ID
 * metric - Name of the metric
 * from - Start time for the range for which you want metrics (specified as milli-seconds since epoch)
 * to - End time for the range for which you want metrics (specified as milli-seconds since epoch)
 * resolution - Resolution of data at which you want the points (one of full, 5m, 20m, 60m, 240m, 1440m)
 */
public class GetPoints {
    private static final TimeValue DEFAULT_RANGE = new TimeValue(7, TimeUnit.DAYS);
    private static final Options cliOptions = new Options();
    private static final GnuParser parser = new GnuParser();
    private static final HelpFormatter helpFormatter = new HelpFormatter();
    private static final String TENANT_ID = "tenantId";
    private static final String METRIC = "metric";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String RES = "resolution";

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Tenant ID").create(TENANT_ID));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Metric name").create(METRIC));
        cliOptions.addOption(OptionBuilder.isRequired(false).hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        cliOptions.addOption(OptionBuilder.isRequired(false).hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));
        cliOptions.addOption(OptionBuilder.isRequired(false).hasArg(true)
                .withDescription("Resolution to use: one of 'full, '5m', '30m', '60m', '240m', '1440m'")
                .create(RES));
    }

    public static void main(String args[]) {
        Map<String, Object> options = parseOptions(args);

        Locator locator = Locator.createLocatorFromPathComponents(
                (String) options.get(TENANT_ID),
                (String) options.get(METRIC));

        AstyanaxReader reader = AstyanaxReader.getInstance();

        Long from = (Long) options.get(FROM);
        Long to = (Long) options.get(TO);

        if (from == null || to == null) {
            System.out.println("Either start time or end time is null.");
            to = System.currentTimeMillis();
            from = to - DEFAULT_RANGE.toMillis();
            System.out.println("Using range: " + from + " - " + to);
        }

        if (from >= to) {
            System.err.println("End time " + to + " has to be greater than start time " + from);
            System.exit(2);
        }

        Granularity gran = Granularity.FULL;
        String res = (String) options.get("resolution");
        try {
            gran = Granularity.fromString(res.toLowerCase());
        } catch (Exception ex) {
            System.out.println("Exception mapping resolution to Granularity. Using FULL resolution instead.");
            gran = Granularity.FULL;
        } finally {
            if (gran == null) {
                gran = Granularity.FULL;
            }
        }

        System.out.println("Locator: " + locator + ", from: " + from + ", to: "
                + to + ", resolution: " + gran.shortName());

        MetricData data = reader.getDatapointsForRange(locator, new Range(from, to), gran);
        Map<Long, Points.Point> points = data.getData().getPoints();
        for (Map.Entry<Long, Points.Point> item : points.entrySet()) {
            String output = String.format("Timestamp: %d, Data: %s, Unit: %s", item.getKey(), item.getValue().getData().toString(), data.getUnit());
            System.out.println(output);
        }
    }

    private static Map<String, Object> parseOptions(String[] args) {
        CommandLine line;
        final Map<String, Object> options = new HashMap<String, Object>();
        long now = System.currentTimeMillis();
        options.put(TO, now);
        options.put(FROM, now - DEFAULT_RANGE.toMillis());
        options.put(RES, "full");

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

            if (line.hasOption(RES)) {
                options.put(RES, line.getOptionValue(RES).toLowerCase());
            }
        } catch (ParseException ex) {
            System.err.println("Parse exception " + ex.getMessage());
            helpFormatter.printHelp("GetPoints", cliOptions);
            System.exit(2);
        }

        return options;
    }
}
