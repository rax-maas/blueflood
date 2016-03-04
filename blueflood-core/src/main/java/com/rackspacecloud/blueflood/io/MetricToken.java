package com.rackspacecloud.blueflood.io;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A metric foo.bar.baz.qux, has 4 MetricToken's with values as shown below
 *
 * token=foo, path=foo, isLeaf=false
 * token=bar, path=foo.bar, isLeaf=false
 * token=baz, path=foo.bar.baz, isLeaf=false
 * token=qux, path=foo.bar.baz.qux, isLeaf=true
 *
 */
public final class MetricToken {

    private final String token;
    private final String path;
    private final boolean isLeaf;

    public MetricToken(String path, boolean isLeaf) {
        this.path = path;
        this.isLeaf = isLeaf;

        if (path.lastIndexOf(".") > 0)
            this.token = path.substring(path.lastIndexOf(".") + 1);
        else
            this.token = path;
    }

    public String getToken() {
        return token;
    }

    public String getPath() {
        return path;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricToken metricToken = (MetricToken) o;

        if (isLeaf != metricToken.isLeaf) return false;
        return !(path != null ? !path.equals(metricToken.path) : metricToken.path != null);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(path)
                .append(isLeaf)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "MetricToken{" +
                "token='" + token + '\'' +
                ", path='" + path + '\'' +
                ", isLeaf=" + isLeaf +
                '}';
    }
}
