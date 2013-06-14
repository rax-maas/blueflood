package com.cloudkick.blueflood.internal;


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
