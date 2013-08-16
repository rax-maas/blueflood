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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

public class PaginatedAccountMap {
    private static final Gson gson;
    private List<AccountMapEntry> values;
    private PaginationMetadata metadata;

    static {
        gson = new GsonBuilder().serializeNulls().create();
    }

    public static PaginatedAccountMap fromJSON(String json) {
        return gson.fromJson(json, PaginatedAccountMap.class);
    }

    public List<AccountMapEntry> getEntries() {
        return values;
    }

    public String getNextMarker() {
        return metadata.getNextMarker();
    }
}
