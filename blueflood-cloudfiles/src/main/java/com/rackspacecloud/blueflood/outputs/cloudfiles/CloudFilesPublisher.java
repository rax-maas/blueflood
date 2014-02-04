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

import com.codahale.metrics.Timer;
import com.google.common.io.Closeables;
import com.rackspacecloud.blueflood.service.CloudfilesConfig;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.location.reference.LocationConstants;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class CloudFilesPublisher implements Closeable {
    private final BlobStore blobStore;

    public static final String PROVIDER = "cloudfiles-us";
    public static final String ZONE;

    public static final String USERNAME;
    public static final String API_KEY;
    public static final SimpleDateFormat CONTAINER_DATE_FORMAT;
    public static final Timer uploadTimer = Metrics.timer(CloudFilesPublisher.class, "Rollup Upload Timer");
    private String lastContainerCreated = "";

    static {
        Configuration conf = Configuration.getInstance();
        USERNAME = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_USERNAME);
        API_KEY = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_API_KEY);
        CONTAINER_DATE_FORMAT = new SimpleDateFormat(conf.getStringProperty(CloudfilesConfig.CLOUDFILES_CONTAINER_FORMAT));
        ZONE = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_ZONE);
    }

    public CloudFilesPublisher() {
        Properties overrides = new Properties();
        overrides.setProperty(LocationConstants.PROPERTY_ZONE, ZONE);

        BlobStoreContext context = ContextBuilder.newBuilder(PROVIDER)
                .credentials(USERNAME, API_KEY)
                .overrides(overrides)
                .buildView(BlobStoreContext.class);
        blobStore = context.getBlobStore();
    }

    // idempotent other than when the month changes between two calls
    private void createContainer() {
        String containerName = CONTAINER_DATE_FORMAT.format(new Date());
        blobStore.createContainerInLocation(null, containerName);
        lastContainerCreated = containerName;
    }

    public void close() throws IOException {
        Closeables.close(blobStore.getContext(), true);
    }

    public void publish(String remoteName, byte[] payload) throws IOException {
        Timer.Context ctx = uploadTimer.time();
        try {
            Blob blob = blobStore.blobBuilder(remoteName).payload(payload)
                    .contentType("application/json")
                    .contentEncoding(remoteName.endsWith(".gz") ? "gzip" : "identity")
                    .calculateMD5().build();

            String containerName = CONTAINER_DATE_FORMAT.format(new Date());
            if (!lastContainerCreated.matches(containerName)) {
                createContainer();
            }
            blobStore.putBlob(containerName, blob);
        } finally {
            ctx.stop();
        }
    }
}
