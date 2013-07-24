package com.cloudkick.blueflood.stress;

import com.cloudkick.blueflood.io.NumericSerializer;
import com.cloudkick.blueflood.io.AstyanaxReader;
import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.ServerMetricLocator;
import com.cloudkick.blueflood.utils.Util;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 -acctId ackVCKg1rk
 -entityId enV4V8t4R1
 -checkId ch912swB9i
 -metricName tt_firstbyte
 -monitoringZone mzdfw
 -from 1000
 -to 5000
 -resolution metrics_5m
 */
public class GetPoints {
    
    public static void main(String args[]) {
        Map<String, Object> options = parseOptions(args);
        validate(options);
        
        Locator locator = ServerMetricLocator.createFromTelescopePrimitives(
                (String)options.get("acctId"), 
                (String)options.get("entityId"), 
                (String)options.get("checkId"),
                Util.generateMetricName((String)options.get("metricName"), (String)options.get("monitoringZone")));

        AstyanaxReader reader = AstyanaxReader.getInstance();

        Long from = (Long)options.get("from");
        Long to = (Long)options.get("to");
        System.out.println(String.format("from:%d to:%d", from, to));
        Granularity gran = "string".equals(options.get("resolution")) ? null : (Granularity)options.get("resolution");
        ColumnList<Long> cols;
        if (gran == null) {
            // string.
            cols = reader.getStringPoints(locator, from, to);
        } else {
            // numeric.
            cols = reader.getNumericRollups(locator, gran, from, to);
            // name = timestamp, value = D
        }
        
        for (Column<Long> col : cols) {
            long timestamp = col.getName();
            Object value = gran == null ? col.getValue(StringSerializer.get()) : col.getValue(NumericSerializer.get(gran));
            System.out.println(String.format("%d: %s", timestamp, value.toString()));
        }
    }
    
    private static void validate(Map<String, Object> options) {
        String[] required = {"from", "to", "acctId", "entityId", "checkId", "metricName", "monitoringZone", "resolution"};
            for (String req : required)
                if (!options.containsKey(req))
                    throw new RuntimeException(String.format("Missing '%s'", req));;
    }
    
    // quick and dirty convert args into a map of somewhat useful values.
    public static Map<String, Object> parseOptions(String[] args) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("resolution", "string");
        long now = System.currentTimeMillis() / 1000;
        map.put("to", now);
        map.put("from", now-50000);
        
        for (int i = 0; i < args.length; i++) {
            String key = args[i].substring(1);
            if ("from".equals(key) || "to".equals(key)) {
                map.put(key, Long.parseLong(args[i+1]));
                i += 1;
            } else if ("port".equals(key)) {
                map.put(key, Integer.parseInt(args[i+1]));
                i += 1;
            } else if ("resolution".equals(key)) {
                if (!"string".equals(args[i+1]))
                    map.put(key, Granularity.fromString(args[i+1]));
                else
                    map.put(key, args[i+1]);
                i += 1;
            } else if ("acctId".equals(key) || "entityId".equals(key) || "checkId".equals(key) || "metricName".equals(key) || "monitoringZone".equals(key) || "host".equals(key)) {
                map.put(key, args[i+1]);
                i += 1;
            }
        }
        return map;
    }
}
