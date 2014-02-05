/*
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.cloudfiles;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.rackspacecloud.blueflood.eventemitter.RollupEvent;
import com.rackspacecloud.blueflood.service.CloudfilesConfig;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jclouds.rest.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class StorageManager {
    private final Configuration config = Configuration.getInstance();
    private final File bufferDir;
    private final int maxBufferAge;
    private final int maxBufferSize;
    private static final int UPLOAD_RETRY_INTERVAL = 30000;
    private final BlockingQueue<RollupFile> done = new LinkedBlockingQueue<RollupFile>();
    private RollupFile current;
    private Thread uploaderThread;
    private DoneFileUploader fileUploader;

    private Meter fileCreationMeter = Metrics.meter(StorageManager.class, "Rollup Files Created");
    private Meter rollupEventsSeen = Metrics.meter(StorageManager.class, "Rollup Events Received");
    private Meter uploadExceptionMeter = Metrics.meter(StorageManager.class, "Rollup Remote Upload Exception");
    private Meter rollupWriteFailures = Metrics.meter(StorageManager.class, "Rollup Event Local Write Failures");
    private Gauge<Integer> uploadQueueDepthGauge;

    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    public StorageManager() throws IOException {
        this.maxBufferAge = config.getIntegerProperty(CloudfilesConfig.CLOUDFILES_MAX_BUFFER_AGE);
        this.maxBufferSize = config.getIntegerProperty(CloudfilesConfig.CLOUDFILES_MAX_BUFFER_SIZE);
        this.bufferDir = new File(config.getStringProperty(CloudfilesConfig.CLOUDFILES_BUFFER_DIR));
        this.uploadQueueDepthGauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return done.size();
            }
        };

        Metrics.getRegistry().register(MetricRegistry.name(StorageManager.class, "Upload Queue Depth"), this.uploadQueueDepthGauge);

        if (!bufferDir.isDirectory()) {
            throw new IOException("Specified BUFFER_DIR is not a directory: " + bufferDir.getAbsolutePath());
        }

        File[] bufferFiles = bufferDir.listFiles(RollupFile.fileFilter);
        LinkedList<RollupFile> rollupFileList = new LinkedList<RollupFile>();

        // Build a list of all buffer files in the directory
        for (File bufferFile : bufferFiles) {
            rollupFileList.add(new RollupFile(bufferFile));
        }

        Collections.sort(rollupFileList);

        // Take the newest metric file as the "current" one, or make a new one if there are none
        if (!rollupFileList.isEmpty()) {
            current = rollupFileList.removeLast();
        } else {
            current = RollupFile.buildRollupFile(bufferDir);
        }

        // Queue the rest for upload
        done.addAll(rollupFileList);
    }

    /**
     * Start background storage management and uploading tasks.
     */
    public synchronized void start() {
        if (uploaderThread != null) {
            throw new RuntimeException("StorageManager is already started");
        }

        fileUploader = new DoneFileUploader();
        uploaderThread = new Thread(fileUploader, "StorageManager uploader");
        uploaderThread.start();
    }

    /**
     * Stop background storage management.
     * @throws IOException
     */
    public synchronized void stop() throws IOException {
        if (uploaderThread == null) {
            throw new RuntimeException("Not running");
        }

        uploaderThread.interrupt();
        uploaderThread = null;
        fileUploader.shutdown();
    }

    public synchronized void store(RollupEvent... events) throws IOException {
        if (current.getAge() > maxBufferAge) {
            log.info("buffer file reached age limit, rotating: {}", current.getName());
            rotateCurrent();
        } else if (current.getSize() > maxBufferSize) {
            log.info("buffer file reached size limit, rotating: {}", current.getName());
            rotateCurrent();
        }

        for (RollupEvent event : events) {
            rollupEventsSeen.mark();
            try {
                current.append(event);
            } catch (Exception e) {
                rollupWriteFailures.mark();
                log.error("Could not locally persist rollupEvent, throwing away.", event, e);
            }
        }
    }

    private synchronized void rotateCurrent() throws IOException {
        current.close();
        done.add(current);
        current = RollupFile.buildRollupFile(bufferDir);
        fileCreationMeter.mark();
    }

    private class DoneFileUploader implements Runnable {
        private CloudFilesPublisher publisher;
        private Gzipper gzipper = new Gzipper();

        public DoneFileUploader() {
            resetPublisher();
        }

        @Override
        public void run() {
            /**
             * Process from the pending file queue until we are interrupted. The queue's take method will throw an
             * InterruptedException when this thread is interrupted, however it will clear the thread's interrupted flag
             * in doing so. When we catch an InterruptedException we restore the thread's interrupted state before
             * returning in case anyone up the call stack is curious as to what happened. See:
             * http://www.ibm.com/developerworks/java/library/j-jtp05236/index.html
             */
            try {
                while (true) {
                    uploadAndDeleteFile(done.take());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void resetPublisher() {
            if (this.publisher != null) {
                try {
                    this.publisher.close();
                } catch (IOException e) {
                    log.warn("Error closing down existing publisher", e);
                }
            }
            this.publisher = new CloudFilesPublisher();
        }

        private void shutdown() throws IOException {
            this.publisher.close();
        }


        private synchronized void uploadAndDeleteFile(RollupFile file) throws InterruptedException {
            while (true) {
                try {
                    InputStream fileStream = file.asReadStream();
                    publisher.publish(file.getRemoteName() + ".gz", gzipper.gzip(fileStream));
                    file.delete();
                    break;
                } catch (FileNotFoundException e) {
                    log.error("File could not be found to be deleted.", e);
                    break; // assume file is already gone, so just break
                } catch (IllegalAccessException e) {
                    log.error("File exists but could not be deleted.", e);
                    break; // prevent getting stuck in a loop of re-uploading the file indefinitely
                } catch (IOException e) {
                    log.error("error reading or removing metric file, ignoring", e);
                    uploadExceptionMeter.mark();
                    break;
                } catch (AuthorizationException e) {
                    log.error("Authorization error uploading metric file. let's make a new publisher", e);
                    uploadExceptionMeter.mark();
                    resetPublisher();
                } catch (RuntimeException e) {
                    /**
                     * These are *probably* jclouds exceptions, but they make it very hard to know.
                     */
                    log.error("Error uploading RollupFile", e);
                    uploadExceptionMeter.mark();
                }

                Thread.sleep(UPLOAD_RETRY_INTERVAL);
            }

            log.info("uploaded and removed metric file {}", file.getName());
        }
    }
}
