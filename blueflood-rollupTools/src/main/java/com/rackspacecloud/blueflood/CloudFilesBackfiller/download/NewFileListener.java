package com.rackspacecloud.blueflood.CloudFilesBackfiller.download;

import java.io.File;

public interface NewFileListener {
    public void fileReceived(File f);
}
