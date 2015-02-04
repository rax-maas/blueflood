package com.rackspacecloud.blueflood.service.udp;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.types.DataType;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is simply _ONE_WAY_ of serializing a metric to be sent in a UDP packet.
 */
public class UDPMetricSerialization {
    
    public static Collection<Metric> fromDatagram(DatagramPacket msg) throws IOException {
        final ByteBuf bb = msg.content();
        byte[] buf = new byte[bb.readableBytes()];
        
        bb.readBytes(buf);
        
        CodedInputStream in = CodedInputStream.newInstance(buf);
        final int numMetrics = in.readRawVarint32();
        List<Metric> metrics = new ArrayList<Metric>(numMetrics);
        
        for (int i = 0; i < numMetrics; i++) {
        
            String tenantId = in.readString();
            String metricName = in.readString();
            DataType type = new DataType(in.readString());
            long collectionTime = in.readRawVarint64();
            int ttlSecs = in.readRawVarint32();
            String units = in.readString();
            Object value = null;
            if (type.equals(DataType.BOOLEAN))
                value = in.readBool();
            else if (type.equals(DataType.DOUBLE))
                value = in.readDouble();
            else if (type.equals(DataType.INT))
                value = in.readRawVarint32();
            else if (type.equals(DataType.LONG))
                value = in.readRawVarint64();
            else if (type.equals(DataType.STRING))
                value = in.readString();
            
            if (value != null) {
                metrics.add(new Metric(
                        Locator.createLocatorFromPathComponents(tenantId, metricName), 
                        value, 
                        collectionTime, 
                        new TimeValue(ttlSecs, TimeUnit.SECONDS), 
                        units));
            }
        }
        
        return metrics;
    }
    
    // returns a mutable buffer. you can damage these bytes!
    public static byte[] toBytes(Collection<Metric> metrics) throws IOException {
        AccessibleByteArrayOutputStream out = new AccessibleByteArrayOutputStream(UDPMetricSerialization.computeBinarySize(metrics));
        CodedOutputStream codedOut = CodedOutputStream.newInstance(out);
        codedOut.writeRawVarint32(metrics.size());
        for (Metric metric : metrics) {
            write(metric, codedOut);
        }
        codedOut.flush();
        out.close();
        return out.getUnderlyingBuffer();
    }
    
    private static int computeBinarySize(Collection<Metric> metrics) {
        int size = 0;
        
        // number of each metric.
        size += CodedOutputStream.computeRawVarint32Size(metrics.size());

        // size of each metric.
        for (Metric metric : metrics) {
            DataType dataType = getDataType(metric);
            size += CodedOutputStream.computeStringSizeNoTag(metric.getLocator().getTenantId());
            size += CodedOutputStream.computeStringSizeNoTag(metric.getLocator().getMetricName());
            size += CodedOutputStream.computeStringSizeNoTag(dataType.toString());
            size += CodedOutputStream.computeRawVarint64Size(metric.getCollectionTime());
            size += CodedOutputStream.computeRawVarint32Size(metric.getTtlInSeconds());
            size += CodedOutputStream.computeStringSizeNoTag(metric.getUnit());
            if (metric.getDataType().equals(DataType.STRING))
                size += CodedOutputStream.computeStringSizeNoTag((String)metric.getMetricValue());
            if (dataType.equals(DataType.INT))
                size += CodedOutputStream.computeRawVarint32Size((Integer) metric.getMetricValue());
            if (dataType.equals(DataType.LONG))
                size += CodedOutputStream.computeRawVarint64Size((Long) metric.getMetricValue());
            if (dataType.equals(DataType.DOUBLE))
                size += CodedOutputStream.computeDoubleSizeNoTag((Double) metric.getMetricValue());
            if (metric.getDataType().equals(DataType.BOOLEAN))
                size += CodedOutputStream.computeBoolSizeNoTag((Boolean) metric.getMetricValue());
        }
        
        return size;
    }

    private static void write(Metric metric, CodedOutputStream out) throws IOException {

        out.writeStringNoTag(metric.getLocator().getTenantId());
        out.writeStringNoTag(metric.getLocator().getMetricName());
        DataType dataType = getDataType(metric);

        out.writeStringNoTag(dataType.toString());
        out.writeRawVarint64(metric.getCollectionTime());
        out.writeRawVarint32(metric.getTtlInSeconds());
        out.writeStringNoTag(metric.getUnit());
        if (metric.getDataType().equals(DataType.STRING))
            out.writeStringNoTag((String) metric.getMetricValue());
        else if (dataType.equals(DataType.INT))
            out.writeRawVarint32((Integer)metric.getMetricValue());
        else if (dataType.equals(DataType.LONG))
            out.writeRawVarint64((Long) metric.getMetricValue());
        else if (dataType.equals(DataType.DOUBLE))
            out.writeDoubleNoTag((Double) metric.getMetricValue());
        else if (metric.getDataType().equals(DataType.BOOLEAN))
            out.writeBoolNoTag((Boolean) metric.getMetricValue());
    }

    private static DataType getDataType(Metric metric) {
        DataType d = null;

        if (metric.getMetricValue() instanceof Integer) {
            d = DataType.INT;
        } else if (metric.getMetricValue() instanceof Long) {
            d = DataType.LONG;
        } else if (metric.getMetricValue() instanceof Double) {
            d = DataType.DOUBLE;
        } else if (metric.getMetricValue() instanceof BigInteger) {
            d = DataType.BIGINT;
        }
        return d;
    }

    // sneaky, but avoids a copy.
    // this makes the assumption that the entire buffer is interesting or has been written to.
    // if allocate N bytes but only put N-M bytes, you still get all N bytes back from getUnderlyingBuffer().
    private static class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {
        public AccessibleByteArrayOutputStream(int size) {
            super(size);
        }
        
        public byte[] getUnderlyingBuffer() {
            return this.buf;
        }
    }
}
