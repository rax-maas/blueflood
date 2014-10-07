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
import com.rackspacecloud.blueflood.CloudFilesBackfiller.service.BackFillerConfig;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.types.Range;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payload;
import org.jclouds.location.reference.LocationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CloudFilesManager implements FileManager {

    private static final Logger log = LoggerFactory.getLogger(CloudFilesManager.class);
    private static final int BUF_SIZE = 0x00100000; // 1MB.
    private static final String lastMarkerPath = ".last_marker"; // Marker really important for this tool. Manually start hacking till we start seeing files
    private final String user;
    private final String key;
    private final String provider; // = "cloudfiles-us";
    private final String zone; // = "IAD";
    private final String container; // = "metric-data-archive";
    private final int batchSize;
    private final List<NewFileListener> listeners = new ArrayList<NewFileListener>();
    
    private String lastMarker = MarkerUtils.readLastMarker();
    private ExecutorService downloadWorkers = Executors.newFixedThreadPool(5);

    public static final long START_TIME = Configuration.getInstance().getLongProperty(BackFillerConfig.REPLAY_PERIOD_START);
    public static final long STOP_TIME = Configuration.getInstance().getLongProperty(BackFillerConfig.REPLAY_PERIOD_STOP);;

    public static final Iterable<Range> ranges = Range.rangesForInterval(Granularity.MIN_5, START_TIME, STOP_TIME);

    private static Timer downloadTimer = Metrics.timer(CloudFilesManager.class, "Cloud Files Download Timer");
    
    public CloudFilesManager(String user, String key, String provider, String zone, String container, int batchSize) {
        this.user = user;
        this.key = key;
        this.provider = provider;
        this.zone = zone;
        this.container = container;
        this.batchSize = batchSize;
    }

    public synchronized boolean hasNewFiles() {
        // see if there are any files since lastMarker.
        BlobStoreContext ctx = ContextBuilder.newBuilder(provider)
                .credentials(user, key)
                .overrides(new Properties() {{
                    setProperty(LocationConstants.PROPERTY_ZONE, zone);
                }})
                .buildView(BlobStoreContext.class);
        
        BlobStore store = ctx.getBlobStore();
        ListContainerOptions options = new ListContainerOptions().maxResults(batchSize).afterMarker(lastMarker);
        PageSet<? extends StorageMetadata> pages = store.list(container, options);
        
        log.debug("Saw {} new files since {}", pages.size() == batchSize ? "many" : Integer.toString(pages.size()), lastMarker);
        boolean emptiness = getBlobsWithinRange(pages).isEmpty();

        if(emptiness) {
            log.warn("No file found within range {}", new Range(START_TIME, STOP_TIME));
        } else {
            log.debug("New files found within range {}", new Range(START_TIME, STOP_TIME));
        }

        return !emptiness;
    }

    private NavigableMap<Long,String> getBlobsWithinRange(PageSet<? extends StorageMetadata> pages) {
        // TreeMap used because of sorted property
        TreeMap<Long, String> tsToBlobName = new TreeMap<Long, String>();
        for (StorageMetadata blobMeta : pages) {
            String fileName = blobMeta.getName(); // 20140226_1393442533000.json.gz
            String dateAndTs = fileName.split("\\.", 2)[0].trim(); // 20140226_1393442533000
            String tsCreated = dateAndTs.split("_")[1].trim(); // 1393442533000
            long ts = Long.parseLong(tsCreated);
            tsToBlobName.put(ts, fileName);
        }
        //Gets key within the time range specified
        NavigableMap<Long, String> mapWithinRange = tsToBlobName.subMap(START_TIME - 60000*15, true, STOP_TIME + 60000*30, true);
        if(mapWithinRange.isEmpty()) {
            lastMarker = tsToBlobName.lastEntry().getValue().trim();
            synchronized (CloudFilesManager.this) {
                // this is where we resume from.
                MarkerUtils.writeLastMarker(tsToBlobName.lastEntry().getValue().trim());
            }
        }
        return mapWithinRange;
    }
    
    private class BlobDownload implements Callable<String> {
        
        private final File downloadDir;
        private final String container;
        private final String name;
        private final BlobStore store;
        
        public BlobDownload(File downloadDir, BlobStore store, String container, String name) {
            this.downloadDir = downloadDir;
            this.store = store;
            this.container = container;
            this.name = name;
        }
        
        public String call() throws Exception {
            Blob blob = store.getBlob(container, name);
            Payload payload = blob.getPayload();
            InputStream is = payload.getInput();
            File tempFile = new File(downloadDir, name + ".tmp");
            Timer.Context downloadContext = downloadTimer.time();
            try {
                long read = 0;
                long length = payload.getContentMetadata().getContentLength();
                OutputStream out = new FileOutputStream(tempFile, false);
                byte[] buf = new byte[BUF_SIZE];
                while (read < length) {
                    int avail = Math.min(is.available(), BUF_SIZE);
                    if (avail < 0) {
                        try { Thread.sleep(100); } catch (Exception ex) {}
                    } else {
                        int readLength = is.read(buf);
                        read += readLength;
                        out.write(buf, 0, readLength);
                    }
                }
                out.flush();
                out.close();
                File permFile = new File(downloadDir, name);
                if (tempFile.renameTo(permFile)) {
                    notifyListeners(permFile);
                } else {
                    throw new IOException("Could not rename file");
                }
            } catch (IOException ex) {
                tempFile.delete();
            } finally {
                payload.release();
                downloadContext.stop();
            }
            return name;
        }
    }
    
    public synchronized void downloadNewFiles(File downloadDir) {
        log.info("Downloading new files since {}", lastMarker);
        
        BlobStoreContext ctx = ContextBuilder.newBuilder(provider)
                .credentials(user, key)
                .overrides(new Properties() {{
                    setProperty(LocationConstants.PROPERTY_ZONE, zone);
                }})
                .buildView(BlobStoreContext.class);

        // threadsafe according to https://jclouds.apache.org/documentation/userguide/blobstore-guide/
        BlobStore store = ctx.getBlobStore();
        ListContainerOptions options = new ListContainerOptions().maxResults(batchSize).afterMarker(lastMarker);
        PageSet<? extends StorageMetadata> pages = store.list(container, options);

        //Gets key within the time range specified
        NavigableMap<Long, String> mapWithinRange = getBlobsWithinRange(pages);

        //Download only for keys within that range
        for(Map.Entry<Long, String> blobMeta : mapWithinRange.entrySet()) {
            log.info("Downloading file: " + blobMeta.getValue());
            downloadWorkers.submit(new BlobDownload(downloadDir, store, container, blobMeta.getValue()));
            lastMarker = blobMeta.getValue();
            synchronized (CloudFilesManager.this) {
                // this is where we resume from.
                MarkerUtils.writeLastMarker(blobMeta.getValue());
            }
        }
        log.info("Updated the last marker value as " + lastMarker);
    }

    public void addNewFileListener(NewFileListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }
    
    private void notifyListeners(File newFile) {
        for (NewFileListener listener : listeners) {
            try {
                listener.fileReceived(newFile);
            } catch (Throwable allSortsOfBadness) {
                log.error(allSortsOfBadness.getMessage(), allSortsOfBadness);
            }
        }
    }
    
    private static class MarkerUtils {
        private static final Lock lock = new ReentrantLock();
        
        public static String readLastMarker() {
            lock.lock();
            try {
                File f = new File(lastMarkerPath);
                if (!f.exists())
                    return "";
                else {
                    try {
                        byte[] buf = new byte[(int)f.length()];
                        InputStream in = new FileInputStream(f);
                        int read = 0;
                        while (read < buf.length)
                            read += in.read(buf, read, buf.length - read);
                        in.close();
                        return new String(buf).trim();
                    } catch (Throwable th) {
                        log.error(th.getMessage(), th);
                        return "";
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        
        public static void writeLastMarker(String s) {
            lock.lock();
            try {
                OutputStream out = new FileOutputStream(new File(lastMarkerPath));
                out.write(s.getBytes());
                out.close();
            } catch (Throwable th) {
                log.error(th.getMessage(), th);
            } finally {
                lock.unlock();
            }
        }
    }
}