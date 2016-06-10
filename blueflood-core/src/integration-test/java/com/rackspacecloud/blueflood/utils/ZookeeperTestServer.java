package com.rackspacecloud.blueflood.utils;

import com.google.common.io.Files;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Test Server for zookeeper. Allows you to instantiate a zookeeper instance.
 */
public class ZookeeperTestServer {
    private volatile File dataDir;
    private volatile File logDir;
    private volatile ZooKeeperServer zkServer;
    private volatile NIOServerCnxnFactory connectionFactory;

    /**
     * The port used to start Zookeeper server. Starts at zero so we pick an ephemeral port first time
     * through (and then update this field). Subsequent restarts of the server will then remain on the
     * chosen port, to test clients' disconnect/reconnect.
     */
    private volatile int port = 0;

    public ZookeeperTestServer() {
        dataDir = Files.createTempDir();
        logDir = Files.createTempDir();
    }

    public String getZkConnect() {
        return String.format("localhost:%d", zkServer.getClientPort());
    }

    public void connect() throws IOException, InterruptedException {
        zkServer = new ZooKeeperServer(new FileTxnSnapLog(dataDir, logDir), new ZooKeeperServer.BasicDataTreeBuilder());
        connectionFactory = new NIOServerCnxnFactory();
        connectionFactory.configure(new InetSocketAddress(port), 10);
        connectionFactory.startup(zkServer);
        port = zkServer.getClientPort();
    }

    public void disconnect() {
        if (connectionFactory != null) {
            connectionFactory.shutdown();
            connectionFactory = null;
        }
    }

    public void shutdown() {
        disconnect();
        if (zkServer != null) {
            zkServer.shutdown();
            zkServer = null;
        }
        FileUtils.deleteRecursive(logDir);
        FileUtils.deleteRecursive(dataDir);
    }

    public void expire(long sessionId) throws IOException, InterruptedException {
        if (zkServer != null) {
            zkServer.closeSession(sessionId);
        }
    }
}
