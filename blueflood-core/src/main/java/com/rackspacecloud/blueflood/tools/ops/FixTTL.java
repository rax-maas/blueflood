package com.rackspacecloud.blueflood.tools.ops;

import org.apache.commons.cli.*;
import com.netflix.astyanax.model.*;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.types.*;
import java.util.*;

public class FixTTL {
    // Update all the TTL's for a set of metrics over a particular time range
    public static void fixTTLs(ColumnFamily CF, String tenantID,
                               List<String> metrics, Range range, Integer newTTL, Boolean dryRun)
            throws Exception {
        Integer counter = 0;

        List<Locator> locators = new ArrayList<Locator>();
        for (String path: metrics) {
            locators.add(Locator.createLocatorFromPathComponents(tenantID, path));
        }
        System.out.println("Starting FixTTL");
        if (dryRun) {
            System.out.println("This is a dry run");
        }
        // Print out current TTL's, and update them
        Map<Locator, ColumnList<Long>> data =
                AstyanaxReader.getInstance().getColumnsFromDB(locators, CF, range);

        for (Map.Entry<Locator, ColumnList<Long>> entry: data.entrySet()) {
            Locator l = entry.getKey();
            System.out.println("Current Locator is " + l.toString() + "; counter is " + counter);
            counter++;
            ColumnList<Long> cols = entry.getValue();

            System.out.println("Number of cols is " + cols.size());
            for (Column<Long> col: cols) {
                System.out.println("col name is " + col.getName() + " ttl is " + col.getTtl());
            }
            if (!dryRun) {
                AstyanaxWriter.getInstance().updateTTL(CF, l, cols, newTTL);
            }
        }

        System.out.println("\n\nThe updated TTL's are:");
        counter = 0;
        data = AstyanaxReader.getInstance().getColumnsFromDB(locators, CF, range);
        for (Map.Entry<Locator, ColumnList<Long>> entry: data.entrySet()) {
            Locator l = entry.getKey();
            System.out.println("Locator of updated metric is " + l.toString());
            ColumnList<Long> cols = entry.getValue();
            for (Column<Long> col: cols) {
                System.out.println("col name is " + col.getName() + "; The new ttl is " + col.getTtl() + "; the counter is " + counter);
            }
            counter++;
        }
        System.out.println("FixTTL done");
    }

    public static void main(String args[]) throws Exception {
        Map<String, Object> options = OptionsHandler.parseOptions(args);
        ColumnFamily CF = CassandraModel.CF_METRICS_FULL;
        Range range = new Range((Long) options.get(OptionsHandler.FROM),
				(Long) options.get(OptionsHandler.TO));
        String tenantID = (String) options.get(OptionsHandler.TENANT_ID);
        Integer newTTL = (Integer) options.get(OptionsHandler.TTL);
        List<String> metrics = (List<String>) options.get(OptionsHandler.METRIC_LIST);
        Boolean dryRun = (Boolean) options.get(OptionsHandler.DRY_RUN);
        fixTTLs(CF, tenantID, metrics, range, newTTL, dryRun);
        System.exit(0);
    }
}


class OptionsHandler {
    static final Options cliOptions = new Options();
    static final GnuParser parser = new GnuParser();
    static final HelpFormatter helpFormatter = new HelpFormatter();
    static final String TENANT_ID = "tenantId";
    static final String METRIC_LIST = "metricList";
    static final String FROM = "from";
    static final String TO = "to";
    static final String TTL = "ttl";
    static final String DRY_RUN = "dryRun";
    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Tenant ID").create(TENANT_ID));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Comma separated list of metrics").create(METRIC_LIST));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("TTL in seconds").create(TTL));
        cliOptions.addOption(OptionBuilder
                .withDescription("Don't include if you really want to update").create(DRY_RUN));
    }
    static Map<String, Object> parseOptions(String[] args) {
        CommandLine line;
        final Map<String, Object> options = new HashMap<String, Object>();

        try {
            line = parser.parse(cliOptions, args);

            if (line.hasOption(TENANT_ID)) {
                options.put(TENANT_ID, line.getOptionValue(TENANT_ID));
            }

            if (line.hasOption(METRIC_LIST)) {
                String metrics[] = line.getOptionValue(METRIC_LIST).split(",");
                options.put(METRIC_LIST, Arrays.asList(metrics));
            }

            if (line.hasOption(FROM)) {
                options.put(FROM, new Long(line.getOptionValue(FROM)));
            }

            if (line.hasOption(TO)) {
                options.put(TO, new Long(line.getOptionValue(TO)));
            }

            if (line.hasOption(TTL)) {
                options.put(TTL, new Integer(line.getOptionValue(TTL)));
            }
            if (line.hasOption(DRY_RUN)) {
                options.put(DRY_RUN, true);
            } else {
                options.put(DRY_RUN, false);
            }
        } catch (ParseException ex) {
            System.err.println("Parse exception " + ex.getMessage());
            helpFormatter.printHelp("Fix TTL", cliOptions);
            System.exit(2);
        }
        return options;
    }
}
