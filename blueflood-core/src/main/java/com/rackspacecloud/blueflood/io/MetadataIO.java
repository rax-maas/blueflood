package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Map;

public interface MetadataIO {
    public void write(Locator locator, String key, String value) throws IOException;
    public Map<String, String> getAllValues(Locator locator) throws IOException;
    
    // only used in unit tests.
    public int getNumberOfRowsTest() throws IOException;
    
    // todo: consider the utility of a method: public String get(Locator locator, String key) throws IOException
}
