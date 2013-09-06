package com.rackspacecloud.blueflood.service.udp;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * This is simply _ONE_WAY_ of serializing a metric to be sent in a UDP packet.
 */
public class UDPMetricSerialization {
    
    public static Metric fromDatagram(DatagramPacket msg) throws IOException {
        final ByteBuf bb = msg.content();
        byte[] buf = new byte[bb.readableBytes()];
        bb.readBytes(buf);
        CodedInputStream in = CodedInputStream.newInstance(buf);
        String tenantId = in.readString();
        String metricName = in.readString();
        Metric.Type type = new Metric.Type(in.readString());
        long collectionTime = in.readRawVarint64();
        int ttlSecs = in.readRawVarint32();
        String units = in.readString();
        Object value = null;
        if (type.equals(Metric.Type.BOOLEAN))
            value = in.readBool();
        else if (type.equals(Metric.Type.DOUBLE))
            value = in.readDouble();
        else if (type.equals(Metric.Type.INT))
            value = in.readRawVarint32();
        else if (type.equals(Metric.Type.LONG))
            value = in.readRawVarint64();
        else if (type.equals(Metric.Type.STRING))
            value = in.readString();
        return new Metric(
                Locator.createLocatorFromPathComponents(tenantId, metricName), 
                value, 
                collectionTime, 
                new TimeValue(ttlSecs, TimeUnit.SECONDS), 
                units);
    }
    
    // returns a mutable buffer. you can damage these bytes!
    public static byte[] toBytes(Metric metric) throws IOException {
        AccessibleByteArrayOutputStream out = new AccessibleByteArrayOutputStream(UDPMetricSerialization.computeBinarySize(metric));
        write(metric, out);
        out.flush();
        out.close();
        return out.getUnderlyingBuffer();
    }
    
    private static int computeBinarySize(Metric metric) {
        int size = 0;
        size += CodedOutputStream.computeStringSizeNoTag(metric.getLocator().getTenantId());
        size += CodedOutputStream.computeStringSizeNoTag(metric.getLocator().getMetricName());
        size += CodedOutputStream.computeStringSizeNoTag(metric.getType().toString());
        size += CodedOutputStream.computeRawVarint64Size(metric.getCollectionTime());
        size += CodedOutputStream.computeRawVarint32Size(metric.getTtlInSeconds());
        size += CodedOutputStream.computeStringSizeNoTag(metric.getUnit());
        if (metric.getType().equals(Metric.Type.STRING))
            size += CodedOutputStream.computeStringSizeNoTag((String)metric.getValue());
        if (metric.getType().equals(Metric.Type.INT))
            size += CodedOutputStream.computeRawVarint32Size((Integer) metric.getValue());
        if (metric.getType().equals(Metric.Type.LONG))
            size += CodedOutputStream.computeRawVarint64Size((Long) metric.getValue());
        if (metric.getType().equals(Metric.Type.DOUBLE))
            size += CodedOutputStream.computeDoubleSizeNoTag((Double) metric.getValue());
        if (metric.getType().equals(Metric.Type.BOOLEAN))
            size += CodedOutputStream.computeBoolSizeNoTag((Boolean) metric.getValue());
        return size;
    }
    private static void write(Metric metric, OutputStream src) throws IOException {
        CodedOutputStream out = CodedOutputStream.newInstance(src);
        
        out.writeStringNoTag(metric.getLocator().getTenantId());
        out.writeStringNoTag(metric.getLocator().getMetricName());
        out.writeStringNoTag(metric.getType().toString());
        out.writeRawVarint64(metric.getCollectionTime());
        out.writeRawVarint32(metric.getTtlInSeconds());
        out.writeStringNoTag(metric.getUnit());
        if (metric.getType().equals(Metric.Type.STRING))
            out.writeStringNoTag((String) metric.getValue());
        else if (metric.getType().equals(Metric.Type.INT))
            out.writeRawVarint32((Integer)metric.getValue());
        else if (metric.getType().equals(Metric.Type.LONG))
            out.writeRawVarint64((Long)metric.getValue());
        else if (metric.getType().equals(Metric.Type.DOUBLE))
            out.writeDoubleNoTag((Double) metric.getValue());
        else if (metric.getType().equals(Metric.Type.BOOLEAN))
            out.writeBoolNoTag((Boolean) metric.getValue());
        
        out.flush();
    }
    
    // sneaky, but avoids a copy.
    private static class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {
        public AccessibleByteArrayOutputStream(int size) {
            super(size);
        }
        
        public byte[] getUnderlyingBuffer() {
            return this.buf;
        }
    }
}
