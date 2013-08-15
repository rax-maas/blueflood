package com.rackspacecloud.blueflood.internal;

import java.io.IOException;
import java.util.List;

public interface InternalAPI {
    public Account fetchAccount(String tenantId) throws IOException;

    public List<AccountMapEntry> listAccountMapEntries() throws IOException;
}
