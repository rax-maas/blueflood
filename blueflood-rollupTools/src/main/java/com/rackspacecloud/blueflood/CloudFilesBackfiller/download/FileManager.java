package com.rackspacecloud.blueflood.CloudFilesBackfiller.download;

import java.io.File;

public interface FileManager {
    public boolean hasNewFiles();
    public void downloadNewFiles(File downloadDir);
    public void addNewFileListener(NewFileListener listener);
}
