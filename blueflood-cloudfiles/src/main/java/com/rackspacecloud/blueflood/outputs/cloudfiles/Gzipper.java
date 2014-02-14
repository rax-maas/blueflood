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
import com.rackspacecloud.blueflood.utils.Metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

public class Gzipper {
    InputStream jsonFileIn = null;
    ByteArrayOutputStream gzippedPayload = null;
    GZIPOutputStream gZCompressor = null;
    int nbRead;
    Timer timer = Metrics.timer(Gzipper.class, "Gzip Timer");
    // Used to read the stream faster
    byte[] buf = new byte[1024 * 256 ];

    public synchronized byte[] gzip(InputStream input) throws IOException {
        Timer.Context ctx = timer.time();
        try {
            gzippedPayload = new ByteArrayOutputStream();
            gZCompressor = new GZIPOutputStream(gzippedPayload);
            while ((nbRead = input.read(buf)) != -1) {
                gZCompressor.write(buf, 0, nbRead);
            }

            input.close();

            gZCompressor.finish();
            gZCompressor.close();

            return gzippedPayload.toByteArray();
        } finally {
            ctx.stop();
        }
    }
}
