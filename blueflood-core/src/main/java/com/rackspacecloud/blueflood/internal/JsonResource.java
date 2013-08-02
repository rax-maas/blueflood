package com.rackspacecloud.blueflood.internal;

import java.io.IOException;

interface JsonResource {
    public String getResource(String name) throws IOException;
}
