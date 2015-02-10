package com.rackspacecloud.blueflood.types;

import java.util.HashMap;
import java.util.Map;

public class Event {
    private long when = 0;
    private String what = "";
    private String data = "";
    private String tags = "";


    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("when", getWhen());
        map.put("what", getWhat());
        map.put("data", getData());
        map.put("tags", getTags());
        return map;
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
