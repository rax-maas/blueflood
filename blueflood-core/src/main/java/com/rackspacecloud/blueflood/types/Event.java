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
