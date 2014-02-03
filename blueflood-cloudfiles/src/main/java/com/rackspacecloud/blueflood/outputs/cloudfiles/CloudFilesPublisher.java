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
import org.jclouds.location.reference.LocationConstants;
import org.jclouds.openstack.swift.CommonSwiftAsyncClient;
import org.jclouds.openstack.swift.CommonSwiftClient;
import org.jclouds.openstack.swift.domain.SwiftObject;
import org.jclouds.rest.RestContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CloudFilesPublisher implements Closeable {
    private final BlobStore blobStore;
    private final RestContext<CommonSwiftClient, CommonSwiftAsyncClient> swiftCtx;

    public static final String PROVIDER = "cloudfiles-us";
    public static final String ZONE;

    public static final String USERNAME;
    public static final String API_KEY;
    public static final String CONTAINER;
    public static final Timer uploadTimer = Metrics.timer(CloudFilesPublisher.class, "Rollup Upload Timer");

    static {
        Configuration conf = Configuration.getInstance();
        USERNAME = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_USERNAME);
        API_KEY = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_API_KEY);
        CONTAINER = conf.getStringProperty(CloudfilesConfig.CLOUDFILES_CONTAINER);
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
        swiftCtx = context.unwrap();
        createContainer();
    }

    // idempotent
    private void createContainer() {
        swiftCtx.getApi().createContainer(CONTAINER);
    }

    public void close() throws IOException {
        Closeables.close(blobStore.getContext(), true);
    }

    public void publish(String remoteName, InputStream fileStream) {
        Timer.Context ctx = uploadTimer.time();
        try {
            SwiftObject object = swiftCtx.getApi().newSwiftObject();
            object.getInfo().setName(remoteName);
            object.setPayload(fileStream);

            swiftCtx.getApi().putObject(CONTAINER, object);
        } finally {
            ctx.stop();
        }
    }
}
