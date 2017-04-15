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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BluefloodTool {
    private static final Options entryOptions = new Options();
    private static final GnuParser parser = new GnuParser();
    private static final HelpFormatter helpFormatter = new HelpFormatter();
    private static final String TENANT_ID = "tenantId";
    private static final String METRIC = "metric";
    private static final String MODE = "mode";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String RES = "resolution";
    private static final String fromFile = "fromFile";
    //TODO : currently unused option. Will be used later in querying
    private static final String toFile = "toFile";
    private static CommandLine line;
    private static BF_Mode bfMode;
    private static Locator locator;

    private enum BF_Mode{
        INGEST,
        QUERY,
        ROLLUP
    }

    static {
        entryOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Tenant ID").create(TENANT_ID));
        entryOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Metric name").create(METRIC));
        entryOptions.addOption(OptionBuilder.hasArg(true).isRequired().withDescription("Select operation mode : INGEST, QUERY, ROLLUP").create("mode"));
    }

    public static void main(String[] args) {
        try {
            line = parser.parse(entryOptions, args);
        } catch (ParseException e) {
            System.err.println("Error encountered while parsing options: "+e.getMessage());
            helpFormatter.printHelp("BluefloodTool", entryOptions);
            System.exit(2);
        }

        locator = Locator.createLocatorFromPathComponents(
                line.getOptionValue(TENANT_ID),
                line.getOptionValue(METRIC));

        for(BF_Mode mode : BF_Mode.values()) {

            if(line.getOptionValue(MODE).equals(mode.name())){
                bfMode = mode;
                break;
            }

        }

        if(bfMode == null) {
            System.err.println("Supplied Blueflood Operational Mode unknown");
            helpFormatter.printHelp("BluefloodTool", entryOptions);
            System.exit(2);
        }

        if(bfMode.name().equals(BF_Mode.INGEST.name()))
            handleIngestMode(args);
        else if(bfMode.name().equals(BF_Mode.QUERY.name()))
            handleQueryMode(args);
        else
            handleRollupMode(args);
    }

    private static void handleRollupMode(String[] args) {
        Options rollupOptions = new Options();
        rollupOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        rollupOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));

        try {
            //setting true will ignore other options than the ones supplied
            line = parser.parse(rollupOptions, args, true);
        } catch (ParseException e) {
            System.err.println("Error encountered while parsing rollup options: " + e.getMessage());
            helpFormatter.printHelp("BluefloodTool", rollupOptions);
            System.exit(2);
        }

        RollupTool.rerollData(locator,
                new Range(Long.getLong(line.getOptionValue(FROM)), Long.getLong(line.getOptionValue(TO))));
    }

    private static void handleQueryMode(String[] args) {
        Options queryOptions = new Options();
        queryOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        queryOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));
        queryOptions.addOption(OptionBuilder.isRequired(false).hasArg(true)
                .withDescription("Resolution to use: one of 'full, '5m', '30m', '60m', '240m', '1440m'")
                .create(RES));
        queryOptions.addOption(OptionBuilder.isRequired(false).hasArg(true)
                .withDescription("File to write the data to")
                .create());

        try {
            line = parser.parse(queryOptions, args, true);
        } catch (ParseException e) {
            System.err.println("Error encountered while parsing rollup options: " + e.getMessage());
            helpFormatter.printHelp("BluefloodTool", queryOptions);
            System.exit(2);
        }

        Granularity gran = null;
        String res = line.getOptionValue(RES);

        try {
            gran = Granularity.fromString(res.toLowerCase());
        } catch (Exception ex) {
            System.out.println("Exception mapping resolution to Granularity. Using FULL resolution instead.");
        } finally {
            if (gran == null) {
                gran = Granularity.FULL;
            }
        }

        //TODO : Add option to write to a file, if we decide on writing in the ingestion like format. Also, possible compression?
        MetricData data = AstyanaxReader.getInstance().getDatapointsForRange(locator,
                new Range(Long.getLong(line.getOptionValue(FROM)), Long.getLong(line.getOptionValue(TO))),
                gran);
        Map<Long, Points.Point> points = data.getData().getPoints();
        for (Map.Entry<Long, Points.Point> item : points.entrySet()) {
            String output = String.format("Timestamp: %d, Data: %s, Unit: %s", item.getKey(), item.getValue().getData().toString(), data.getUnit());
            System.out.println(output);
        }
    }

    private static void handleIngestMode(String[] args) {
        Options ingestOptions = new Options();
        ingestOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("File To Ingest from").create(fromFile));

        try {
            line = parser.parse(ingestOptions, args, true);
        } catch (ParseException e) {
            System.err.println("Error encountered while parsing ingest options: " + e.getMessage());
            helpFormatter.printHelp("BluefloodTool", ingestOptions);
            System.exit(2);
        }

        JsonReader reader = null;
        String tenantId = null;
        List<JSONMetricsContainer.JSONMetric> metricCollection = null;

        //Use the streaming api of JSON as the file object might be too large to load in memory at a time

        try {
            //Force decoding to default charset i.e UTF_8 while reading
            reader = new JsonReader(new InputStreamReader(new FileInputStream(line.getOptionValue(fromFile)), Constants.DEFAULT_CHARSET));
        } catch (FileNotFoundException e) {
            String errOutput = String.format("Supplied file path %s cannot be found", line.getOptionValue(fromFile));
            System.err.println(errOutput);
            System.exit(2);
        }
        //TODO 1) Missing validation of file  2)Metric Metadata processing
        try {
            reader.beginObject();
            String tenantIdKey = reader.nextName();
            if(tenantIdKey != null && tenantIdKey.equals("tenantId")) {
                tenantId = reader.nextString();
            }
            if(!reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
                System.err.println("Supplied file not in expected format");
                System.exit(2);
            }

            reader.beginArray();
            Gson gson = null;
            metricCollection = new ArrayList<JSONMetricsContainer.JSONMetric>();

            while(reader.hasNext()) {
                JSONMetricsContainer.JSONMetric metric = gson.fromJson(reader, Metric.class);
                metricCollection.add(metric);
            }

            reader.endArray();
            reader.close();
        } catch (IOException e) {
            System.err.println("IOException encountered while parsing from JSON : "+e.getMessage());
            System.exit(-1);
        }

        JSONMetricsContainer container = new JSONMetricsContainer(tenantId, metricCollection);
        List<Metric> containerMetrics = container.toMetrics();

        try {
            AstyanaxWriter.getInstance().insertMetrics(new ArrayList<IMetric>(containerMetrics), CassandraModel.CF_METRICS_FULL);
        } catch (ConnectionException e) {
            System.err.println("Error encountered while ingesting metrics into cassandra : "+e.getMessage());
            System.exit(-1);
        }
    }
}
