package com.rackspacecloud.blueflood.CloudFilesBackfiller.service;

import com.rackspacecloud.blueflood.CloudFilesBackfiller.download.CloudFilesManager;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.download.DownloadService;
import com.rackspacecloud.blueflood.CloudFilesBackfiller.download.FileManager;
import com.rackspacecloud.blueflood.service.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class RangeDownloader {
    private static final Logger log = LoggerFactory.getLogger(RangeDownloader.class);
    private static Configuration config = Configuration.getInstance();

    public static void main(String args[]) {
        final String USER = config.getStringProperty(BackFillerConfig.CLOUDFILES_USER);
        final String KEY = config.getStringProperty(BackFillerConfig.CLOUDFILES_KEY);
        final String PROVIDER = config.getStringProperty(BackFillerConfig.CLOUDFILES_PROVIDER);
        final String ZONE = config.getStringProperty(BackFillerConfig.CLOUDFILES_ZONE);
        final String CONTAINER = config.getStringProperty(BackFillerConfig.CLOUDFILES_CONTAINER);

        final File downloadDir = new File(config.getStringProperty(BackFillerConfig.DOWNLOAD_DIR));
        downloadDir.mkdirs();

        // connect the download service to the file manager.
        FileManager fileManager = new CloudFilesManager(USER, KEY, PROVIDER, ZONE, CONTAINER, config.getIntegerProperty(BackFillerConfig.BATCH_SIZE));
        DownloadService downloadService = new DownloadService(downloadDir);
        downloadService.setFileManager(fileManager);

        // delete any temp files before starting.
        for (File tmp : downloadDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".json.gz.tmp");
            }
        })) {
            if (!tmp.delete()) {
                log.error("Could not delete a temp file %s", tmp.getName());
                System.exit(-1);
            }
        }

        try {
            downloadService.start();
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
}
