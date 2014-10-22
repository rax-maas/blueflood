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

 * Original author: gdusbabek
 * Modified by: chinmay
 */

package com.rackspacecloud.blueflood.CloudFilesBackfiller.download;

import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DownloadService {
    private static final Logger log = LoggerFactory.getLogger(DownloadService.class);
    private static final int MAX_FILES = 5;
    private static final int MAX_UNEXPECTED_ERRORS = 5;
    private final File downloadDir;
    private final Thread thread;
    private final Lock downloadLock = new ReentrantLock(true);
    private FileManager fileManager = null; // todo: should be final.
    private static Timer waitingTimer = Metrics.timer(DownloadService.class, "Delay introduced because cloud files were not consumed/merged");
    
    private boolean running = false;
    private boolean terminated = false;
    
    private int unexpectedErrors = 0;
    
    
    public DownloadService(final File downloadDir) {
        this.downloadDir = downloadDir;
        this.running = false;
        this.thread = new Thread("Download Service") {
            public void run() {
                while (!terminated) {
                    // if there are > 1 tmp files in the dir, just wait.
                    FilenameFilter filter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.endsWith(".json.gz.tmp");
                        }
                    };
                    while (downloadDir.listFiles(filter).length > 1) {
                        try { 
                            sleep(200L);
                        } catch (InterruptedException ex) {
                            Thread.interrupted();
                        }
                    }
                    doCheck();
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                }
                log.debug("Download service thread stopping");
            }
        };
        this.thread.start();
    }
    
    public void setFileManager(FileManager newFileManager) {
        downloadLock.lock();
        try {
            fileManager = newFileManager;
        } finally {
            downloadLock.unlock();
        }
    }
    
    public synchronized void start() throws IOException {
        if (terminated)
            throw new IOException("Download service has been terminated. It cannot be restarted.");
        if (running)
            throw new IOException("Download service is already running.");
        if (!downloadDir.exists())
            throw new IOException("Download directory does not exist");
        
        log.info("Resuming downloads");
        running = true;
        thread.interrupt();
    }
    
    public synchronized void stop() {
        log.info("Stopping download service");
        running = false;
        thread.interrupt();
    }
    
    public synchronized void terminate(boolean waitFor) {
        terminated = true;
        stop();
        log.info("Terminating download service");
        thread.interrupt();
        if (waitFor) {
            log.info("Wating for download service termination");
            while (thread.isInterrupted() || thread.isAlive()) {
                try { Thread.sleep(100); } catch (Exception ex) {}
            }
            log.info("Download service terminated");
        }
    }
    
    // gets run by the thread.
    private void doCheck() {
        if (!running) return;
        if (fileManager == null) return;
        
        if (unexpectedErrors > MAX_UNEXPECTED_ERRORS) {
            log.info("Terminating because of errors");
            terminate(false);
            return;
        }

        Timer.Context waitTimerContext = waitingTimer.time();
        // Possible infinite thread sleep? This will make sure we fire downloading only when are the files are consumed/merged
        while (downloadDir.listFiles().length != 0) {
            log.debug("Waiting for files in download directory to clear up. Sleeping for 1 min. If you see this persistently, it means the downloaded files are not getting merged properly/timely");
            try { Thread.sleep(60000); } catch (Exception ex) {}
        }
        waitTimerContext.stop();
        if (downloadLock.tryLock()) {
            try {
                if (fileManager.hasNewFiles()) {
                    fileManager.downloadNewFiles(downloadDir);
                }
            } catch (Throwable unexpected) {
                unexpectedErrors += 1;
                log.error("UNEXPECTED; WILL TRY TO RECOVER");
                log.error(unexpected.getMessage(), unexpected);
                // sleep for a minute?
                if (Thread.interrupted()) {
                    try {
                        thread.sleep(60000);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    }
                }
            } finally {
                downloadLock.unlock();
            }
        } else {
            log.debug("Download in progress");
        }
    }
}
