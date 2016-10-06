package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Collection;

public interface DelayedLocatorIO {

    /**
     * Insert a delayed locator for a SlotKey
     *
     * @param locator
     * @throws IOException
     */
    public void insertLocator(Granularity g, int slot, Locator locator) throws IOException;

    /**
     * @param slotKey
     * @return a collection of the locators objects corresponding to the given SlotKey
     * @throws IOException
     */
    public Collection<Locator> getLocators(SlotKey slotKey) throws IOException;
}
