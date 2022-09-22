package com.rackspacecloud.blueflood.utils;

import java.util.concurrent.Future;

public class TestServers {
    public static void ensureStarted() throws Exception {
        Future<Boolean> esStart = ElasticsearchTestServer.getInstance().asyncEnsureStarted();
        Future<Boolean> cassStart = CassandraTestServer.getInstance().asyncEnsureStarted();
        esStart.get();
        cassStart.get();
    }
}
