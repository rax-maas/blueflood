/*
 * Copyright 2013 Rackspace
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

package com.rackspacecloud.blueflood.internal;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** fetches json from a http host */
class HttpJsonResource implements JsonResource {
    private static final Logger log = LoggerFactory.getLogger(HttpJsonResource.class);
    private static final ResponseHandler<String> STRING_HANDLER = new BasicResponseHandler();
    private static final Timer requestTimer = Metrics.newTimer(HttpJsonResource.class, "Internal API Request Timer",
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    private final List<String> cluster = new ArrayList<String>();
    private final String basePath;
    private final DefaultHttpClient client;

    HttpJsonResource(ClientConnectionManager connectionManager, String clusterString, String basePath) {
        Collections.addAll(this.cluster, clusterString.split(",", -1));
        this.basePath = basePath;

        client = new DefaultHttpClient(connectionManager);
        client.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, true);
        client.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
        client.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 30000);

        // wait this long for an available connection. setting this correctly is important in order to avoid
        // connectionpool timeouts. Chances are that anybody having to wait this long is hating life. 
        client.getParams().setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, 5000);
    }

    public String getResource(String name) throws IOException {
        ClusterException clusterEx = null;

        ArrayList<String> rcluster = new ArrayList<String>(this.cluster);
        Collections.shuffle(rcluster);

        for (String host : rcluster) {
            HttpGet get = new HttpGet("http://" + host + basePath + name);
            final TimerContext requestCtx = requestTimer.time();
            try {
                log.debug("Fetching {}", get.getURI());
                return client.execute(get, STRING_HANDLER);
            }  catch (HttpResponseException ex) {
                // the idea here is that this class of error should bubble up and leave the underlying HTTP connection
                // in tact.  (Also, the same error would likely happen if another host is called contacted--no point in
                // going on to the next host in the cluster.
                requestCtx.stop();
                throw ex;
            } catch (IOException ex) {
                if (clusterEx == null)
                    clusterEx = new ClusterException();
                clusterEx.append(host, ex);
            } finally {
                requestCtx.stop();
                get.releaseConnection(); // important for pooled connection reuse.
            }
        }
        throw clusterEx;
    }
}
