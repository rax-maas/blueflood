package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Map;

public class AstyanaxMetadataIO implements MetadataIO {
    
    public AstyanaxMetadataIO() { }
    
    @Override
    public void put(Locator locator, String key, String value) throws IOException {
        try {
            AstyanaxWriter.getInstance().writeMetadataValue(locator, key, value);
        } catch (RuntimeException ex) {
            throw new IOException(ex);
        } catch (ConnectionException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Map<String, String> getAllValues(Locator locator) throws IOException {
        try {
            return AstyanaxReader.getInstance().getMetadataValues(locator);
        } catch (RuntimeException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public int getNumberOfRowsTest() throws IOException {
        throw new IOException("Not implemented");
    }
}
