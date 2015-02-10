package com.rackspacecloud.blueflood.types;

import java.util.HashMap;
import java.util.Map;

public class Event {
    private long when = 0;
    private String what = "";
    private String data = "";
    private String tags = "";

    public static enum FieldLabels {
        when,
        what,
        data,
        tags,
        tenantId
    }

    public static final String untilParameterName = "until";
    public static final String fromParameterName = "from";
    public static final String tagsParameterName = FieldLabels.tags.name();

    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {
            {
                put(FieldLabels.when.name(), getWhen());
                put(FieldLabels.what.name(), getWhat());
                put(FieldLabels.data.name(), getData());
                put(FieldLabels.tags.name(), getTags());
            }
        };
    }

    public long getWhen() {
        return when;
    }

    public void setWhen(long when) {
        this.when = when;
    }

    public String getWhat() {
        return what;
    }

    public void setWhat(String what) {
        this.what = what;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }
}
