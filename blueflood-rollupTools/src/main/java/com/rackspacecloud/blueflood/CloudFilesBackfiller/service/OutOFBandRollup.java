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
package com.rackspacecloud.blueflood.CloudFilesBackfiller.service;

import com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers.FileHandler;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.rollup.handlers.RollupGenerator;
import com.rackspacecloud.blueflood.service.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;

public class OutOFBandRollup {
    private static final Logger log = LoggerFactory.getLogger(OutOFBandRollup.class);
    private static final FileHandler fileHandler = new FileHandler();
    private static Thread monitoringThread;
    private static Thread rollupGeneratorThread;
    private static boolean running;

    public static void main(String args[]) {

        final File file = new File(Configuration.getInstance().getStringProperty(BackFillerConfig.DOWNLOAD_DIR));

        if(!file.exists() || !file.isDirectory()) {
            log.error("Download directory not properly configured");
            System.exit(-1);
        }

        File rollupDir = new File(Configuration.getInstance().getStringProperty(BackFillerConfig.ROLLUP_DIR));
        rollupDir.mkdirs();

        running = true;

        monitoringThread = new Thread(){

                public void run() {

                    while(running) {

                        log.info("Scanning for downloaded files");
                        FilenameFilter filter = new FilenameFilter() {
                            public boolean accept(File dir, String name) {
                                return name.endsWith(".json.gz");
                            }
                        };
                        File[] cloudFiles = file.listFiles(filter);


                        Comparator<File> fileComparator = new Comparator<File>() {
                            @Override
                            public int compare(File f1, File f2) {
                                return (int) (getTimeStamp(f1) - getTimeStamp(f2));
                            }

                            private long getTimeStamp(File file) {
                                String fileName = file.getName(); // 20140226_1393442533000.json.gz
                                String dateAndTs = fileName.split("\\.", 2)[0].trim(); // 20140226_1393442533000
                                String tsCreated = dateAndTs.split("_")[1].trim(); // 1393442533000
                                return Long.parseLong(tsCreated);
                            }
                        };
                        // Throttled the cloud files being parsed
                        if (cloudFiles.length != 0 && RollupGenerator.rollupExecutors.getQueue().size() < 10000) {
                            Arrays.sort(cloudFiles, fileComparator);
                            CountDownLatch latch = new CountDownLatch(cloudFiles.length);
                            fileHandler.setCountDown(latch);

                            for(File file : cloudFiles) {
                                fileHandler.fileReceived(file);
                            }
                            try {
                                log.info("Waiting for latch to be released");
                                latch.await();
                            } catch (InterruptedException e) {
                            }
                        } else {
                            log.info("Sleeping for some time because cloud files have not yet cleared/rollup queue size is way too high");
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                running = false;
                            }
                        }
                    }

                }
        };

        monitoringThread.start();

        rollupGeneratorThread = new Thread(new RollupGenerator());
        rollupGeneratorThread.start();
    }

    public static Thread getMonitoringThread() {
        return monitoringThread;
    }

    public static Thread getRollupGeneratorThread() {
        return rollupGeneratorThread;
    }
}
