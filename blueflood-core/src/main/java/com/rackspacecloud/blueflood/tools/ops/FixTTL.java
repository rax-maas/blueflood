package com.rackspacecloud.blueflood.tools.ops;

import org.apache.commons.cli.*;
import com.netflix.astyanax.model.*;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class FixTTL {
    private static final Options cliOptions = new Options();
    private static final GnuParser parser = new GnuParser();
    private static final HelpFormatter helpFormatter = new HelpFormatter();
    private static final String TENANT_ID = "tenantId";
    private static final String METRIC_LIST = "metricList";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String TTL = "ttl";


    private static final Logger log = LoggerFactory.getLogger(FixTTL.class);

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Tenant ID").create(TENANT_ID));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withDescription("Comma separated list of metrics").create(METRIC_LIST));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("Start timestamp (millis since epoch)").create(FROM));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("End timestamp (millis since epoch)").create(TO));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true)
                .withDescription("TTL in seconds").create(TTL));
    }
    private static Map<String, Object> parseOptions(String[] args) {
        CommandLine line;
        final Map<String, Object> options = new HashMap<String, Object>();

        try {
            line = parser.parse(cliOptions, args);

            if (line.hasOption(TENANT_ID)) {
                options.put(TENANT_ID, line.getOptionValue(TENANT_ID));
            }

            if (line.hasOption(METRIC_LIST)) {
	        options.put(METRIC_LIST, line.getOptionValue(METRIC_LIST).split(","));
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

        } catch (ParseException ex) {
            System.err.println("Parse exception " + ex.getMessage());
            helpFormatter.printHelp("Fix TTL", cliOptions);
            System.exit(2);
        }
        return options;
    }

    public static void main(String args[]) {
        Map<String, Object> options = parseOptions(args);
        ColumnFamily CF = CassandraModel.CF_METRICS_FULL;
        Range range = new Range((Long) options.get(FROM),
				(Long) options.get(TO));
        String tenantID = (String) options.get(TENANT_ID);
        Integer newTTL = (Integer) options.get(TTL);
        String metrics[] = (String[]) options.get(METRIC_LIST);
        List<Locator> locators = new ArrayList<Locator>();
        for (String path: metrics) {
            locators.add(Locator.createLocatorFromPathComponents(tenantID, path));
        }
        Map<Locator, ColumnList<Long>> data = AstyanaxReader.getInstance().getColumnsFromDB(locators, CF, range);
        for (Map.Entry<Locator, ColumnList<Long>> entry: data.entrySet()) {
            Locator l = entry.getKey();
            log.info("Current Locator is " + l.toString());
            ColumnList<Long> cols = entry.getValue();

            log.info("Number of cols is " + cols.size());
            for (Column<Long> col: cols) {
                log.info("col name is " + col.getName() + " ttl is " + col.getTtl());
            }
            AstyanaxWriter.getInstance().updateTTL(CF, l, cols, newTTL);
        }
        data = AstyanaxReader.getInstance().getColumnsFromDB(locators, CF, range);
        for (Map.Entry<Locator, ColumnList<Long>> entry: data.entrySet()) {
            Locator l = entry.getKey();
            log.info("Locator of updated metric is " + l.toString());
            ColumnList<Long> cols = entry.getValue();
            for (Column<Long> col: cols) {
                log.info("col name is " + col.getName() + " new ttl is " + col.getTtl());
            }
        }
        log.info("FixTTL done");
    }
}
