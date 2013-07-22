package com.cloudkick.blueflood.stress;

import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.ServerMetricLocator;
import com.cloudkick.blueflood.utils.Util;
import com.cloudkick.cep.util.Configuration;
import com.cloudkick.util.MetricHelper;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import telescope.thrift.*;

import java.util.Map;

/** queries a telescope server. Pass in args 
  
 -acctId ackVCKg1rk
 -entityId enV4V8t4R1
 -checkId ch912swB9i
 -metricName tt_firstbyte
 -monitoringZone mzdfw
 -resolution metrics_5m
 -from 1000
 -to 5000
 -host 127.0.0.1 
 -port 2467 
  
 * */
public class GetPointsTelescope {
    
    public static void main(String args[]) {
        Map<String, Object> options = GetPoints.parseOptions(args);
        validate(options);
        
        
        TTransport transport = new TFramedTransport.Factory(Configuration.getIntegerProperty("THRIFT_LENGTH")).getTransport(new TSocket((String)options.get("host"), (Integer)options.get("port")));
        TProtocol proto = new TBinaryProtocol(transport);
        RollupServer.Client client = new RollupServer.Client(proto);
        try {
            transport.open();
        } catch (TTransportException ex) {
            ex.printStackTrace();
            System.exit(-1);
        }

        Long from = (Long)options.get("from");
        Long to = (Long)options.get("to");
        System.out.println(String.format("from:%d to:%d", from, to));
        Resolution res = "string".equals(options.get("resolution")) ? null : fromGranularity((Granularity) options.get("resolution"));

        String mz = (String) options.get("monitoringZone");
        //Currently metricName has two meanings. We'll call one metricLabel temporarily for now.
        String metricLabel = (String) options.get("metricName");
        String accountId = (String) options.get("acctId");
        if (mz != null) {
            metricLabel = String.format("%s.%s", mz, metricLabel);
        }
        String metricName = ServerMetricLocator.createFromTelescopePrimitives(accountId, (String)options.get("entityId"), 
        		(String)options.get("checkId"), (String)metricLabel).getMetricName();

        try {
            RollupMetrics metrics = client.GetDataByResolution(
                accountId,
                metricName,
                from,
                to,
                res
            );
            for (RollupMetric metric : metrics.getMetrics()) {
                System.out.println(String.format("%d: %s", metric.getTimestamp(), toString(metric)));
            }
        } catch (TException ex) {
            ex.printStackTrace();
        }
    }
    
    private static void validate(Map<String, Object> options) {
        String[] required = {"from", "to", "acctId", "entityId", "checkId", "metricName", "monitoringZone", "resolution", "host", "port"};
            for (String req : required)
                if (!options.containsKey(req))
                    throw new RuntimeException(String.format("Missing '%s'", req));;
    }
    
    private static String toString(RollupMetric metric) {
        return String.format("cnt: %d, raw: %s, avg: %s, min: %s, max: %s, var: %s", 
                metric.getNumPoints(), 
                toString(metric.getRawSample()),
                toString(metric.getAverage()), 
                toString(metric.getMin()),
                toString(metric.getMax()),
                toString(metric.getVariance()));
    }
    
    private static String toString(Metric metric) {
        if (metric == null) return "null";
        switch (metric.getMetricType()) {
            case MetricHelper.Type.DOUBLE:
                return Util.DECIMAL_FORMAT.format(metric.getValueDbl());
            case MetricHelper.Type.INT32:
                return Integer.toString(metric.getValueI32());
            case MetricHelper.Type.INT64:
            case MetricHelper.Type.UINT64:
            case MetricHelper.Type.UINT32:
                return Long.toString(metric.getValueI64());
            case MetricHelper.Type.STRING:
                return metric.getValueStr();
            default:
                return "Unknown metric type: " + (char)metric.getMetricType();
        }
    }
    
    private static Resolution fromGranularity(Granularity g) {
        if (g == Granularity.FULL) return Resolution.FULL;
        else if (g == Granularity.MIN_5) return Resolution.MIN5;
        else if (g == Granularity.MIN_20) return Resolution.MIN20;
        else if (g == Granularity.MIN_60) return Resolution.MIN60;
        else if (g == Granularity.MIN_240) return Resolution.MIN240; 
        else if (g == Granularity.MIN_1440) return Resolution.MIN1440;
        else throw new RuntimeException("What? " + g);
    }
}
