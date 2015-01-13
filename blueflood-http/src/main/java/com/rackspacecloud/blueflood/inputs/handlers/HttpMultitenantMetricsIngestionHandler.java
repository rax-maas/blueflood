/*
 * Copyright 2014-2015 Rackspace
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

package com.rackspacecloud.blueflood.inputs.handlers;


import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import java.io.IOException;
import java.util.List;

public class HttpMultitenantMetricsIngestionHandler extends HttpMetricsIngestionHandler {

    public HttpMultitenantMetricsIngestionHandler(HttpMetricsIngestionServer.Processor processor, TimeValue timeout) {
        super(processor, timeout);
    }

    @Override
    protected JSONMetricsContainer createContainer(String body, String tenantId) throws JsonParseException, JsonMappingException, IOException {
        List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                mapper.readValue(
                        body,
                        typeFactory.constructCollectionType(List.class,
                                JSONMetricsContainer.ScopedJSONMetric.class)
                );
        return new JSONMetricsContainer(tenantId, jsonMetrics);
    }
}
