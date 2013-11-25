package com.rackspacecloud.blueflood.statsd;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.StatType;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.CounterRollup;
import com.rackspacecloud.blueflood.types.GaugeRollup;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.types.Rollup;
import com.rackspacecloud.blueflood.types.SetRollup;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import com.rackspacecloud.blueflood.types.TimerRollup;
import com.rackspacecloud.blueflood.utils.Util;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DataTool {
    
    private static class Options {
        private static final String Host = "host";
        private static final String Port = "port";
        private static final String From = "from";
        private static final String To = "to";
        private static final String Resolution = "resolution";
        private static final String Shards = "shards";
        private static final String Locator = "locator";
    }
    
    private static class Commands {
        private static final String GetPoints = "points";
        private static final String GetLocators = "locators";
        private static final String GetMetadata = "metadata";
    }
    
    private static abstract class Command {
        public void applyDefaults(Map<String, String> opts) { }
        
        public void validateOptions(Map<String, String> opts) throws Exception {
            // need cassandra host and port
            expectString(opts, Options.Host);
            expectInteger(opts, Options.Port);
        }
        
        public abstract boolean execute(PrintStream out, PrintStream err, Map<String, String> opts);
        
        public void expectLong(Map<String, String> map, String key) throws Exception {
            try {
                Long.parseLong(map.get(key));
            } catch (NullPointerException ex) {
                throw new Exception(String.format("%s not specified", key));
            } catch (NumberFormatException ex) {
                throw new Exception(String.format("invalid %s (%s)", key, map.get(key)));
            }
        }
        
        public void expectInteger(Map<String, String> map, String key) throws Exception {
            try {
                Integer.parseInt(map.get(key));
            } catch (NullPointerException ex) {
                throw new Exception(String.format("%s not specified", key));
            } catch (NumberFormatException ex) {
                throw new Exception(String.format("invalid %s (%s)", key, map.get(key)));
            }
        }
        
        public void expectString(Map<String, String> map, String key) throws Exception {
            if (map.get(key) == null)
                throw new Exception(String.format("%s not specified", key));
        }
    }
    
    private static class GetLocators extends Command {
        private Collection<Integer> shards;
        @Override
        public void applyDefaults(Map<String, String> opts) {
            super.applyDefaults(opts);
            if (opts.get(Options.Shards) == null)
                opts.put(Options.Shards, "all");
        }

        @Override
        public void validateOptions(Map<String, String> opts) throws Exception {
            super.validateOptions(opts);
            expectString(opts, Options.Shards);
            
            // now set them.
            this.shards = Util.parseShards(opts.get(Options.Shards));
        }

        @Override
        public boolean execute(PrintStream out, PrintStream err, Map<String, String> opts) {
            for (int shard : this.shards) {
                ColumnList<Locator> locators = DataTool.reader.getAllLocators(shard);
                if (locators.size() == 0) {
                    out.println(String.format("%d: NONE", shard));
                } else {
                    for (Column<Locator> locatorColumn : locators) {
                        out.println(String.format("%d: %s", shard, locatorColumn.getName().toString()));
                    }
                }
            }
            return true;
        }
    }
    
    private static class GetMetadata extends Command {
        @Override
        public void validateOptions(Map<String, String> opts) throws Exception {
            super.validateOptions(opts);
            expectString(opts, Options.Locator);
        }

        @Override
        public boolean execute(PrintStream out, PrintStream err, Map<String, String> opts) {
            Locator locator = Locator.createLocatorFromDbKey(opts.get(Options.Locator));
            ColumnList<String> columns = reader.getAllMetadata(locator);
            out.println(locator.toString());
            out.println("-------------------");
            if (columns.size() == 0) {
                out.println("NONE");
            } else {
                for (Column<String> col : columns) {
                    System.out.println(col.getName() + ":" + col.getStringValue());
                }
            }
            return true;
        }
    }
    
    private static class GetPoints extends Command {
        @Override
        public void applyDefaults(Map<String, String> opts) {
            super.applyDefaults(opts);
            if (opts.get(Options.From) == null) {
                opts.put(Options.From, Long.toString(System.currentTimeMillis() - (24 * 60 * 60 * 1000)));
            }
            
            if (opts.get(Options.To) == null) {
                opts.put(Options.To, Long.toString(System.currentTimeMillis()));
            }
            
            if (opts.get(Options.Resolution) == null) {
                opts.put(Options.Resolution, Granularity.FULL.shortName());
            }
        }

        @Override
        public void validateOptions(Map<String, String> opts) throws Exception {
            super.validateOptions(opts);
            expectLong(opts, Options.From);
            expectLong(opts, Options.To);
            expectString(opts, Options.Locator);
            expectString(opts, Options.Resolution);
        }

        @Override
        public boolean execute(PrintStream out, PrintStream err, Map<String, String> opts) {
            // figure out the type
            try {
                Range range = new Range(Long.parseLong(opts.get(Options.From)), Long.parseLong(opts.get(Options.To)));
                Granularity granularity = Granularity.fromString(opts.get(Options.Resolution));
                Locator locator = Locator.createLocatorFromDbKey(opts.get(Options.Locator));
                StatType type = StatType.fromString(reader.getMetadataValue(locator, StatType.CACHE_KEY).toString());
                ColumnFamily<Locator, Long> columnFamily = GetPoints.columnFamilyOf(type, granularity);
                Class<? extends Rollup> classType = GetPoints.classOf(type, granularity);
                
                Points<? extends Rollup> points = reader.getDataToRoll(classType, locator, range, columnFamily);
                for (Points.Point<? extends Rollup> point : points.getPoints().values()) {
                    System.out.println(String.format("%d: %s", point.getTimestamp(), point.getData().toString()));
                }
                return true;
            } catch (Exception ex) {
                err.println(ex);
                return false;
            }
        }
        
        public static Class<? extends Rollup> classOf(StatType type, Granularity gran) {
            if (type == StatType.COUNTER)
                return CounterRollup.class;
            else if (type == StatType.TIMER)
                return TimerRollup.class;
            else if (type == StatType.SET)
                return SetRollup.class;
            else if (type == StatType.GAUGE)
                return GaugeRollup.class;
            else if (type == StatType.UNKNOWN && gran == Granularity.FULL)
                return SimpleNumber.class;
            else if (type == StatType.UNKNOWN && gran != Granularity.FULL)
                return BasicRollup.class;
            else
                throw new RuntimeException(String.format("Unexpected type/gran combination: %s, %s", type, gran));
        }
        
        public static ColumnFamily<Locator, Long> columnFamilyOf(StatType type, Granularity gran) {
            if (gran == Granularity.FULL) {
                switch (type) {
                    case COUNTER:
                    case TIMER:
                    case GAUGE:
                    case SET:
                        return AstyanaxIO.CF_METRICS_PREAGGREGATED;
                    case UNKNOWN:
                    default:
                        return AstyanaxIO.CF_METRICS_FULL;
                } 
            } else if (gran == Granularity.MIN_5)
                return AstyanaxIO.CF_METRICS_5M;
            else if (gran == Granularity.MIN_20)
                return AstyanaxIO.CF_METRICS_20M;
            else if (gran == Granularity.MIN_60)
                return AstyanaxIO.CF_METRICS_60M;
            else if (gran == Granularity.MIN_240)
                return AstyanaxIO.CF_METRICS_240M;
            else if (gran == Granularity.MIN_1440)
                return AstyanaxIO.CF_METRICS_1440M;
            else
                throw new RuntimeException(String.format("Unexpected type/gran combination: %s, %s", type, gran));
        }
    }
    
    private static PrintStream out;
    private static PrintStream err;
    private static AstyanaxReader reader;

    /**
     * Things you can do:
     * 
     * Show all locators:
     *  java DataTool locators -host 192.168.231.128 -port 19180 -shards 1,3,5,6
     *  java DataTool locators -host 192.168.231.128 -port 19180 -shards all
     *  java DataTool locators -host 192.168.231.128 -port 19180
     *  
     * Dump metadata
     *  java GetDAta metadata -host 192.168.231.128 -port 19180 -locator timer0
     * 
     * Dump metrics
     *  java DataTool points -host 192.168.231.128 -port 19180 -locator timer0
     *  java DataTool points -host 192.168.231.128 -port 19180 -locator timer0 -resolution full
     *  java DataTool points -host 192.168.231.128 -port 19180 -locator timer0 -resolution 5m
     *  
     * You do not have to worry about Configuration.java.  As long as you populate the required fields of the command
     * Configuration.java will be taken care of
     */
    public static void main(String args[]) {
        // for now.
        DataTool.out = System.out;
        DataTool.err = System.out;
        
        if (args.length == 0) {
            DataTool.err.println("No arguments specified");
            System.exit(-1);
        }
        
        String commandString = args[0];
        args = DataTool.shift(args);
        Map<String, String> opts = DataTool.parseOptions(args);
        
        Command command = null;
        if (Commands.GetLocators.equals(commandString))
            command = new GetLocators();
        else if (Commands.GetPoints.equals(commandString))
            command = new GetPoints();
        else if (Commands.GetMetadata.equals(commandString))
            command = new GetMetadata();
        
        command.applyDefaults(opts);
        
        try {
            command.validateOptions(opts);
        } catch (Exception ex) {
            err.println(ex.getMessage());
            System.exit(-1);
        }
        
        // set things up so that AstyanaxReader can be populated.
        // astyanax reader uses configuration class, so we need to do it this way. :(
        System.setProperty("CLUSTER_NAME", "");
        System.setProperty("ROLLUP_KEYSPACE", "DATA");
        System.setProperty("CASSANDRA_MAX_RETRIES", "2");
        System.setProperty("DEFAULT_CASSANDRA_PORT", opts.get(Options.Port));
        System.setProperty("CASSANDRA_HOSTS", String.format("%s:%s", opts.get(Options.Host), opts.get(Options.Port)));
        System.setProperty("MAX_CASSANDRA_CONNECTIONS", "2");
        System.setProperty("MAX_TIMEOUT_WHEN_EXHAUSTED", "2000");
        
        // now set the reader.
        reader = AstyanaxReader.getInstance();
        
        command.execute(DataTool.out, DataTool.err, opts);
        System.exit(0);
    }
    
    private static Map<String, String> parseOptions(String args[]) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < args.length;) {
            if (args[i].startsWith("-")) {
                map.put(args[i].substring(1), args[i+1]);
                i += 2;
            } else if (args[i].indexOf("=") >= 0) {
                String[] parts = args[i].split("=", -1);
                map.put(parts[0], parts[1]);
                i += 1;
            } else {
                out.println("Ignoring option: " + args[i]);
                i += 1;
            }
        }
        return map;
    }
    
    private static String[] shift(String[] a) {
        String[] b = new String[a.length - 1];
        System.arraycopy(a, 1, b, 0, b.length);
        return b;
    }
}
