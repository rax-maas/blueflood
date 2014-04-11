package com.rackspacecloud.blueflood.dw.ingest;

import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream;
import com.google.protobuf.CodedOutputStream;
import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.core.util.ReaderWriter;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/** at some point in the future this can send data to kafka, a commit log, whatever. */
public class LocalDurabilityFilter implements ContainerRequestFilter {
    
    private static final Logger log = LoggerFactory.getLogger(LocalDurabilityFilter.class);
    
    public LocalDurabilityFilter() {
        
    }
    
    @Override
    public ContainerRequest filter(ContainerRequest request) {

        int contentLength = Integer.parseInt(request.getHeaderValue("Content-Length"));
        // make it big enough to hold the path.
        ByteBuffer buf = ByteBuffer.allocateDirect(contentLength + 128);
        CodedOutputStream out = CodedOutputStream.newInstance(new ByteBufferBackedOutputStream(buf));
        InputStream in = request.getEntityInputStream();
        
        try {
            if (in.available() > 0) {
                // copy the json into a byte array. this will get reused later.
                ByteArrayOutputStream jsonBuf = new ByteArrayOutputStream(contentLength);
                ReaderWriter.writeTo(in, jsonBuf);
                byte[] jsonBytes = jsonBuf.toByteArray();
                
                out.writeStringNoTag(request.getAbsolutePath().getPath());
                out.writeRawVarint32(Integer.parseInt(request.getHeaderValue("Content-Length")));
                out.writeRawBytes(jsonBytes);
                out.flush();
                
                buf.flip();
                // all of buf.remaining() is ready to be serialized.
                // todo: send to the durability object, add the receipt as a header.
                
                // we need to put the input back so it can be read.
                request.setEntityInputStream(new ByteArrayInputStream(jsonBytes));
                request.getRequestHeaders().putSingle("x-commit-receipt", "aaaaa");
                request.getQueryParameters().putSingle("commitReceipt", "aaaaa");
                
                // now persist raw.
            }
        } catch (IOException ex) {
            throw new ContainerException(ex);
        }
        
        return request;
    }
    
    
}
