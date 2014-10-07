/*
 Original author : gdusbabek
 Modified by: chinmay
 */
package com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public class FileHandler {

    private static final Logger log = LoggerFactory.getLogger(FileHandler.class);
    protected static final ExecutorService handlerThreadPool = Executors.newFixedThreadPool(5);
    private CountDownLatch latch;


    public void fileReceived(final File f) {

        final Future<File> parseResult = handlerThreadPool.submit(new Callable<File>() {
            public File call() throws Exception {
                log.info("Parsing {}", f.getAbsolutePath());
                BuildStore storeBuilder = BuildStore.getBuilder();
                try {
                    InputStream in = new GZIPInputStream(new FileInputStream(f), 0x00100000);
                    storeBuilder.merge(in);
                    in.close();
                    log.info("Done parsing {}", f.getAbsolutePath());
                } catch (Exception ex) {
                    // something happened during parsing.
                    log.error("Could not parse {} {}", f.getAbsolutePath(), ex);
                } finally {
                    storeBuilder.close();
                }

                try {
                    log.info("Trying to remove " + f.getAbsolutePath());
                    if (!f.delete()) {
                        throw new ExecutionException(new IOException("Cannot delete file: " + f.getAbsolutePath()));
                    }
                    log.info("Removed " + f.getAbsolutePath());
                } catch (ExecutionException ex) {
                    log.error("Could not remove {} {}", f.getAbsolutePath(), ex);
                } finally {
                    latch.countDown();
                }
                return f;
            }
        });
    }

    public void setCountDown(CountDownLatch latch) {
        this.latch = latch;
    }
}
