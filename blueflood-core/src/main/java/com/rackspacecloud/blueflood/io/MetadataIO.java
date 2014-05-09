package com.rackspacecloud.blueflood.io;

import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface MetadataIO {
    public void put(Locator locator, String key, String value) throws IOException;
    public Map<String, String> getAllValues(Locator locator) throws IOException;
    public Table<Locator, String, String> getAllValues(Set<Locator> locators) throws IOException;
    public void putAll(Table<Locator, String, String> meta) throws IOException;
    
    // only used in unit tests.
    public int getNumberOfRowsTest() throws IOException;
    
    // todo: consider the utility of a method: public String get(Locator locator, String key) throws IOException
}
