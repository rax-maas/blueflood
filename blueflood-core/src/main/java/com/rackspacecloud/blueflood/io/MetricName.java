package com.rackspacecloud.blueflood.io;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A metric foo.bar.baz.qux, has 4 {@link MetricName}'s with values as shown below
 *
 * name=foo, isCompleteName=false
 * name=foo.bar, isCompleteName=false
 * name=foo.bar.baz, isCompleteName=false
 * name=foo.bar.baz.qux, isCompleteName=true
 *
 */
public final class MetricName {

    private final String name;
    private final boolean isCompleteName;

    public MetricName(String name, boolean isCompleteName) {
        this.name = name;
        this.isCompleteName = isCompleteName;
    }

    public String getName() {
        return name;
    }

    public boolean isCompleteName() {
        return isCompleteName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricName metricName = (MetricName) o;

        if (isCompleteName != metricName.isCompleteName) return false;
        return !(name != null ? !name.equals(metricName.name) : metricName.name != null);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(name)
                .append(isCompleteName)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "MetricName{" +
                "name='" + name + '\'' +
                ", isCompleteName=" + isCompleteName +
                '}';
    }
}
