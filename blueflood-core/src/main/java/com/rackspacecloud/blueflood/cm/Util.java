package com.rackspacecloud.blueflood.cm;

import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.rollup.Granularity;
import org.apache.commons.codec.digest.DigestUtils;


import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collection;

public class Util {
    public static final String DEFAULT_DIMENSION = "default";
    public static final Format DECIMAL_FORMAT = new DecimalFormat("0.00");

    public static int computeShard(String s) {
        return (int)Long.parseLong(DigestUtils.md5Hex(s).substring(30), 16) % Constants.NUMBER_OF_SHARDS;
    }
    
    public static Collection<Integer> parseShards(String s) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        if ("ALL".equalsIgnoreCase(s)) {
            for (int i = 0; i < Constants.NUMBER_OF_SHARDS; i++)
                list.add(i);
        } else if ("NONE".equalsIgnoreCase(s)) {
            return list;
        } else {
            for (String part : s.split(",", -1)) {
                int i = Integer.parseInt(part.trim());
                if (i >= Constants.NUMBER_OF_SHARDS || i < 0)
                    throw new NumberFormatException("Invalid shard identifier: " + part.trim());
                list.add(i);
            }
        }
        return list;
    }

    public static String formatStateColumnName(Granularity gran, int slot, String state) {
        return String.format("%s,%d,%s", gran.name(), slot, state); 
    }
    
    public static Granularity granularityFromStateCol(String s) {
        String field = s.split(",", -1)[0];
        for (Granularity g : Granularity.granularities())
            if (g.name().startsWith(field))
                return g;
        return null;
    }
    public static int slotFromStateCol(String s) { return Integer.parseInt(s.split(",", -1)[1]); }
    public static String stateFromStateCol(String s) { return s.split(",", -1)[2]; }

    public static String generateMetricName(String metricBase, String monitoringZone) {
        if (monitoringZone == null) {
            return metricBase;
        } else {
            return String.format("%s.%s", monitoringZone, metricBase);
        }
    }

    public static String generateMetricsDiscoveryDBKey(String accountId, String entityId, String checkId) {
        return String.format("%s,%s,%s", accountId, entityId, checkId);
    }

    public static boolean isExternalMetric(String persistedMetric) {
       return persistedMetric.startsWith(Constants.monitoringZonePrefix);
    }

    public static String getDimensionFromKey(String persistedMetric) {
       return persistedMetric.split("\\.", -1)[0];
    }

    public static String getMetricFromKey(String persistedMetric) {
        return persistedMetric.split("\\.", 2)[1];
    }
}
